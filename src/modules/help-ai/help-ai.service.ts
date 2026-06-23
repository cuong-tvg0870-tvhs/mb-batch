import { Injectable, Logger } from '@nestjs/common';
import { createHash, randomUUID } from 'crypto';
import { PrismaService } from '../prisma/prisma.service';

type HelpChapterSnapshotSource = {
  slug: string;
  title: string;
  content: string;
  order: number;
  updatedAt: Date;
};

@Injectable()
export class HelpAiService {
  private readonly logger = new Logger(HelpAiService.name);
  private readonly model = process.env.HELP_CHAT_MODEL || 'gemini-2.5-flash';
  private readonly keys = (process.env.GEMINI_API_KEYS || '')
    .split(',')
    .map((key) => key.trim())
    .filter(Boolean);

  constructor(private readonly prisma: PrismaService) {}

  async refreshHelpKnowledge() {
    const chapters = await this.prisma.helpChapter.findMany({
      select: {
        slug: true,
        title: true,
        content: true,
        order: true,
        updatedAt: true,
      },
      orderBy: { order: 'asc' },
    });

    if (chapters.length === 0) {
      this.logger.warn('No HelpChapter records found for AI knowledge refresh');
      return;
    }

    const sourceHash = this.hashChapters(chapters);
    await this.ensureSnapshotTable();
    const latest = await this.getLatestSnapshot();

    if (latest?.sourceHash === sourceHash) {
      this.logger.log('Help AI knowledge snapshot is unchanged; skipping');
      return;
    }

    const content = await this.buildKnowledgeSnapshot(chapters);
    await this.persistSnapshot(sourceHash, content);
    this.logger.log(
      `Help AI knowledge snapshot refreshed from ${chapters.length} chapters`,
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

  private hashChapters(chapters: HelpChapterSnapshotSource[]) {
    const payload = chapters.map((chapter) => ({
      slug: chapter.slug,
      title: chapter.title,
      content: chapter.content,
      order: chapter.order,
      updatedAt: chapter.updatedAt.toISOString(),
    }));

    return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  }

  private async buildKnowledgeSnapshot(chapters: HelpChapterSnapshotSource[]) {
    const localSnapshot = this.buildLocalSnapshot(chapters);

    if (this.keys.length === 0) {
      this.logger.warn(
        'GEMINI_API_KEYS is not configured; using local help snapshot',
      );
      return localSnapshot;
    }

    try {
      const aiSnapshot = await this.callGeminiForSnapshot(localSnapshot);
      return aiSnapshot || localSnapshot;
    } catch (error: any) {
      this.logger.warn(
        `Gemini help snapshot generation failed; using local snapshot: ${error?.message}`,
      );
      return localSnapshot;
    }
  }

  private buildLocalSnapshot(chapters: HelpChapterSnapshotSource[]) {
    return chapters
      .map((chapter) =>
        [
          `# ${chapter.title}`,
          `Slug: ${chapter.slug}`,
          chapter.content.slice(0, 2400),
        ].join('\n'),
      )
      .join('\n\n---\n\n')
      .slice(0, 24000);
  }

  private async callGeminiForSnapshot(source: string) {
    const key = this.keys[Math.floor(Math.random() * this.keys.length)];
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${this.model}:generateContent?key=${encodeURIComponent(
      key,
    )}`;

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
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
            parts: [{ text: source.slice(0, 28000) }],
          },
        ],
        generationConfig: {
          temperature: 0.1,
        },
      }),
    });

    if (!res.ok) {
      throw new Error(`Gemini snapshot request failed with ${res.status}`);
    }

    const json = await res.json();
    const parts = json?.candidates?.[0]?.content?.parts;
    if (!Array.isArray(parts)) {
      return '';
    }

    return parts
      .map((part) => (typeof part?.text === 'string' ? part.text : ''))
      .join('')
      .trim();
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
