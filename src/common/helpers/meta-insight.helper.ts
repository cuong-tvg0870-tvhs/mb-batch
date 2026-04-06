import { AD_INSIGHT_FIELDS } from '../utils/meta-field';

export class MetaInsightHelper {
  static buildParams({
    level,
    fields,
    since,
    until,
    timeIncrement,
  }: {
    level: 'campaign' | 'adset' | 'ad';
    fields: string[];
    since?: string;
    until?: string;
    timeIncrement?: number | 'all_days';
  }) {
    return {
      level,
      fields: fields.join(','),
      time_range: since && until ? { since, until } : undefined,
      time_increment: timeIncrement,
    };
  }

  static defaultFields = AD_INSIGHT_FIELDS;
}
