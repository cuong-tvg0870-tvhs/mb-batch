import { Injectable, Logger } from '@nestjs/common';
import { createHash, randomUUID } from 'crypto';
import { access, readdir, readFile } from 'fs/promises';
import { basename, extname, join } from 'path';
import { GeminiApiKeyManager } from './gemini-api-key-manager.service';
import { PrismaService } from '../prisma/prisma.service';

type HelpKnowledgeSnapshotSource = {
  slug: string;
  title: string;
  content: string;
  order: number;
  updatedAt: Date;
  source: 'database' | 'static';
};

@Injectable()
export class HelpAiService {
  private readonly logger = new Logger(HelpAiService.name);
  private readonly geminiModel =
    process.env.HELP_CHAT_GEMINI_MODEL ||
    process.env.HELP_CHAT_MODEL ||
    'gemini-2.5-flash';
  private readonly deepSeekModel =
    process.env.HELP_CHAT_DEEPSEEK_MODEL || 'deepseek-v4-flash';
  private readonly deepSeekBaseUrl =
    process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com';
  private readonly staticKnowledgeDirs = [
    join(__dirname, 'knowledge'),
    join(process.cwd(), 'src/modules/help-ai/knowledge'),
  ];

  constructor(
    private readonly prisma: PrismaService,
    private readonly geminiKeyManager: GeminiApiKeyManager,
  ) {}

  async refreshHelpKnowledge() {
    const dbChapters = await this.prisma.helpChapter.findMany({
      select: {
        slug: true,
        title: true,
        content: true,
        order: true,
        updatedAt: true,
      },
      orderBy: { order: 'asc' },
    });
    const staticChapters = await this.getStaticKnowledgeSources();
    const sources = [
      ...dbChapters.map((chapter) => ({
        ...chapter,
        source: 'database' as const,
      })),
      ...staticChapters,
    ].sort((left, right) => left.order - right.order);

    if (sources.length === 0) {
      this.logger.warn(
        'No help knowledge sources found for AI knowledge refresh',
      );
      return;
    }

    const sourceHash = this.hashChapters(sources);
    await this.ensureSnapshotTable();
    const latest = await this.getLatestSnapshot();

    if (latest?.sourceHash === sourceHash) {
      this.logger.log('Help AI knowledge snapshot is unchanged; skipping');
      return;
    }

    const content = await this.buildKnowledgeSnapshot(sources);
    await this.persistSnapshot(sourceHash, content);
    this.logger.log(
      `Help AI knowledge snapshot refreshed from ${dbChapters.length} DB chapters and ${staticChapters.length} static documents`,
    );
  }

  async triagePendingContributions() {
    if (process.env.HELP_AI_TRIAGE_ENABLED !== 'true') {
      return;
    }

    const pending = await this.prisma.contribution.findMany({
      where: { status: 'PENDING' as any },
      select: {
        id: true,
        type: true,
        title: true,
        content: true,
        createdAt: true,
      },
      orderBy: { createdAt: 'desc' },
      take: 20,
    });

    if (pending.length === 0) {
      this.logger.log('No pending contributions to triage');
      return;
    }

    this.logger.log(
      `Contribution triage observed ${pending.length} pending records. Non-destructive enrichment is reserved for the next schema phase.`,
    );
  }

  private hashChapters(chapters: HelpKnowledgeSnapshotSource[]) {
    const payload = chapters.map((chapter) => ({
      slug: chapter.slug,
      title: chapter.title,
      content: chapter.content,
      order: chapter.order,
      updatedAt: chapter.updatedAt.toISOString(),
      source: chapter.source,
    }));

    return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  }

  private async buildKnowledgeSnapshot(
    chapters: HelpKnowledgeSnapshotSource[],
  ) {
    const localSnapshot = this.buildLocalSnapshot(chapters);

    if (!(await this.geminiKeyManager.hasConfiguredKeys())) {
      this.logger.warn(
        'AI API keys are not configured in DB or environment; using local help snapshot',
      );
      return localSnapshot;
    }

    try {
      const aiSnapshot = await this.callGeminiForSnapshot(localSnapshot);
      return aiSnapshot || localSnapshot;
    } catch (error: any) {
      this.logger.warn(
        `AI help snapshot generation failed; using local snapshot: ${error?.message}`,
      );
      return localSnapshot;
    }
  }

  private buildLocalSnapshot(chapters: HelpKnowledgeSnapshotSource[]) {
    return chapters
      .map((chapter) =>
        [
          `# ${chapter.title}`,
          `Slug: ${chapter.slug}`,
          `Source: ${chapter.source}`,
          chapter.content.slice(0, 12000),
        ].join('\n'),
      )
      .join('\n\n---\n\n')
      .slice(0, 60000);
  }

  private async callGeminiForSnapshot(source: string) {
    const body = {
      systemInstruction: {
        parts: [
          {
            text: 'Tạo bản tóm tắt tri thức ngắn gọn bằng tiếng Việt cho chatbot hỗ trợ dashboard MB Auto. Giữ các màn hình, đường dẫn, khái niệm, quy trình và trạng thái quan trọng.',
          },
        ],
      },
      contents: [
        {
          role: 'user',
          parts: [{ text: source.slice(0, 60000) }],
        },
      ],
      generationConfig: {
        temperature: 0.1,
      },
    };

    let lastError: Error | null = null;
    const triedKeyHashes: string[] = [];

    while (true) {
      const keys = await this.geminiKeyManager.getKeyOrder(triedKeyHashes);
      if (keys.length === 0) break;

      for (const key of keys) {
        triedKeyHashes.push(key.keyHash);
        await this.geminiKeyManager.recordAttempt(key);

        if (key.provider === 'deepseek') {
          const res = await fetch(this.deepSeekChatCompletionUrl(), {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${key.apiKey}`,
            },
            body: JSON.stringify(this.buildDeepSeekSnapshotBody(source)),
          });

          if (!res.ok) {
            const error = await this.geminiKeyManager.readAiError(res);
            lastError = new Error(error.message);

            if (this.geminiKeyManager.isQuotaError(error)) {
              await this.geminiKeyManager.freezeForQuota(key, error);
              continue;
            }

            await this.geminiKeyManager.recordFailure(key, error);

            if (this.geminiKeyManager.isAuthError(error)) {
              await this.geminiKeyManager.disableKey(key, error);
              continue;
            }

            if (this.geminiKeyManager.isRetryableStatus(res.status)) {
              continue;
            }

            throw lastError;
          }

          const json = await res.json();
          await this.geminiKeyManager.recordSuccess(key, json);
          return this.extractDeepSeekText(json);
        }

        const url = `https://generativelanguage.googleapis.com/v1beta/models/${this.geminiModel}:generateContent?key=${encodeURIComponent(
          key.apiKey,
        )}`;

        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });

        if (!res.ok) {
          const error = await this.geminiKeyManager.readGeminiError(res);
          lastError = new Error(error.message);

          if (this.geminiKeyManager.isQuotaError(error)) {
            await this.geminiKeyManager.freezeForQuota(key, error);
            continue;
          }

          await this.geminiKeyManager.recordFailure(key, error);

          if (this.geminiKeyManager.isAuthError(error)) {
            await this.geminiKeyManager.disableKey(key, error);
            continue;
          }

          if (this.geminiKeyManager.isRetryableStatus(res.status)) {
            continue;
          }

          throw lastError;
        }

        const json = await res.json();
        await this.geminiKeyManager.recordSuccess(key, json);
        return this.extractCandidateText(json);
      }
    }

    throw (
      lastError ||
      new Error(
        'All AI API keys are quota-limited, near configured limits, or cooling down',
      )
    );
  }

  private extractCandidateText(json: any) {
    const parts = json?.candidates?.[0]?.content?.parts;
    if (!Array.isArray(parts)) {
      return '';
    }

    return parts
      .map((part) => (typeof part?.text === 'string' ? part.text : ''))
      .join('')
      .trim();
  }

  private extractDeepSeekText(json: any) {
    return String(json?.choices?.[0]?.message?.content || '').trim();
  }

  private buildDeepSeekSnapshotBody(source: string) {
    return {
      model: this.deepSeekModel,
      messages: [
        {
          role: 'system',
          content:
            'Tạo bản tóm tắt tri thức ngắn gọn bằng tiếng Việt cho chatbot hỗ trợ dashboard MB Auto. Giữ các màn hình, đường dẫn, khái niệm, quy trình và trạng thái quan trọng.',
        },
        {
          role: 'user',
          content: source.slice(0, 60000),
        },
      ],
      temperature: 0.1,
      stream: false,
      thinking: {
        type: 'disabled',
      },
    };
  }

  private deepSeekChatCompletionUrl() {
    return `${this.deepSeekBaseUrl.replace(/\/+$/, '')}/chat/completions`;
  }

  private async getStaticKnowledgeSources(): Promise<
    HelpKnowledgeSnapshotSource[]
  > {
    for (const knowledgeDir of this.staticKnowledgeDirs) {
      try {
        await access(knowledgeDir);
        return await this.readStaticKnowledgeSources(knowledgeDir);
      } catch {
        continue;
      }
    }

    this.logger.warn(
      `Static help knowledge directories not found: ${this.staticKnowledgeDirs.join(', ')}`,
    );
    return [];
  }

  private async readStaticKnowledgeSources(
    knowledgeDir: string,
  ): Promise<HelpKnowledgeSnapshotSource[]> {
    const fileNames = (await readdir(knowledgeDir))
      .filter((fileName) => fileName.endsWith('.md'))
      .sort();

    const sources = await Promise.all(
      fileNames.map(async (fileName, index) => {
        const content = await readFile(join(knowledgeDir, fileName), 'utf8');

        return {
          slug: this.slugFromFileName(fileName),
          title: this.titleFromMarkdown(fileName, content),
          content: this.normalizeMarkdownKnowledge(content),
          order: 10000 + index,
          updatedAt: new Date(0),
          source: 'static' as const,
        };
      }),
    );

    return sources;
  }

  private titleFromMarkdown(fileName: string, content: string) {
    const heading = content.match(/^#\s+(.+)$/m)?.[1]?.trim();
    if (heading) {
      return this.unescapeMarkdownText(heading);
    }

    return this.slugFromFileName(fileName)
      .split('-')
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  private slugFromFileName(fileName: string) {
    return basename(fileName, extname(fileName))
      .normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '');
  }

  private normalizeMarkdownKnowledge(content: string) {
    return this.unescapeMarkdownText(content)
      .replace(/!\[[^\]]*]\([^)]+\)/g, '')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
  }

  private unescapeMarkdownText(content: string) {
    return content.replace(/\\([().\-&>])/g, '$1');
  }

  private async ensureSnapshotTable() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "HelpAiKnowledgeSnapshot" (
        "id" TEXT PRIMARY KEY,
        "sourceHash" TEXT NOT NULL UNIQUE,
        "content" TEXT NOT NULL,
        "status" TEXT NOT NULL DEFAULT 'READY',
        "generatedBy" TEXT NOT NULL DEFAULT 'mb-batch',
        "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }

  private async getLatestSnapshot() {
    const rows = await this.prisma.$queryRawUnsafe<
      Array<{ sourceHash: string; updatedAt: Date }>
    >(
      'SELECT "sourceHash", "updatedAt" FROM "HelpAiKnowledgeSnapshot" WHERE "status" = $1 ORDER BY "updatedAt" DESC LIMIT 1',
      'READY',
    );
    return rows[0] || null;
  }

  private async persistSnapshot(sourceHash: string, content: string) {
    await this.prisma.$executeRawUnsafe(
      'INSERT INTO "HelpAiKnowledgeSnapshot" ("id", "sourceHash", "content", "status", "generatedBy", "createdAt", "updatedAt") VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT ("sourceHash") DO UPDATE SET "content" = EXCLUDED."content", "status" = EXCLUDED."status", "updatedAt" = CURRENT_TIMESTAMP',
      randomUUID(),
      sourceHash,
      content,
      'READY',
      'mb-batch',
    );
  }
}
