declare type TextField = {
  value?: { text?: string }[];
};

declare type ArrayTextField = {
  text?: string;
}[];

declare type SelectField = {
  value?: any[];
};

declare type LinkField = {
  link?: string;
  text?: string;
}[];

declare type Fields = {
  ['ID Content']?: TextField;
  ['Dự án']?: SelectField;
  ['Thương hiệu']?: SelectField;
  ['Mã sản phẩm']?: TextField;
  ['Sản phẩm (f)']?: TextField;
  ['ID MC']?: ArrayTextField;
  ['Họ và tên']?: TextField;
  ['Link Content']?: LinkField;
  ['Ngày sản xuất']?: string;
};

declare type RecordItem = {
  record_id: string;
  fields?: Fields;
};

declare type FolderRequest = {
  name: string;
  description?: string;
  parentId: string | null;
};

declare type MetaFolderResponse = {
  id: string;
  name: string;
  description?: string;
  creation_time?: string;
};

declare type FolderNode = {
  id: string;
  name: string;
  description?: string;
  creation_time?: string;
  parent_folder?: { id: string; name: string };
  subfolders?: { data: FolderNode[] };
};

declare type CreativeAsset = {
  id: string;
  name?: string;
  hash?: string;
  url?: string;
  video_id?: string;
  width?: number;
  height?: number;
  duration?: number;
  thumbnail?: string;
  video?: { source?: string; id?: string };
  creation_time?: string;
};

declare type CreativeAssetResponse = {
  data?: CreativeAsset[];
  paging?: {
    next?: string;
  };
};
