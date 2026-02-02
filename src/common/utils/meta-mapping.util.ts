export function normalizeMetaId(id?: string) {
  return id?.trim().toUpperCase() || null;
}

export class MetaFatalError extends Error {
  meta: any;
  constructor(message: string, meta: any) {
    super(message);
    this.name = 'MetaFatalError';
    this.meta = meta;
  }
}

export function normalizeMetaError(err: any) {
  const e = err?.error || err;
  return {
    message: e?.message || 'Unknown Meta error',
    code: e?.code,
    subcode: e?.error_subcode,
    type: e?.type,
    fbtraceId: e?.fbtrace_id,
    raw: err,
  };
}
