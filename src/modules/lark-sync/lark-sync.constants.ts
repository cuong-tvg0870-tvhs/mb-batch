export const LARK_SYNC_QUEUE = 'lark-sync-queue';

export const LARK_SYNC_JOBS = {
  SYNC_WORKFLOW: 'sync-workflow',
  META_UPLOAD_WORKFLOW: 'meta-upload-workflow',
  CLEANUP_DUPLICATE_FOLDERS: 'cleanup-duplicate-folders',
};

export interface FolderRequest {
  name: string;
  description?: string;
  parentId?: string | null;
}

export interface MetaFolderResponse {
  id: string;
  name: string;
  description?: string | null;
  parentId?: string | null;
  creation_time?: Date | null;
}
