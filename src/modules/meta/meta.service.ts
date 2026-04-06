import { Injectable } from '@nestjs/common';
import axios from 'axios';
import 'dotenv/config';
import { AdAccount, FacebookAdsApi, User } from 'facebook-nodejs-business-sdk';
import {
  beautyFashionKeywords,
  commonKeywords,
  fetchAll,
  LIMIT_DATA,
} from '../../common/utils';

import { sleep } from '../../common/utils';
import {
  AD_ACCOUNT_FIELDS,
  AD_PIXEL_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MetaService {
  constructor(private prisma: PrismaService) {}

  private initialized = false;

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  async fetchInterestTargeting() {
    // FETCH INTEREST LIST:
    const queries = [
      ...'abcdefghijklmnopqrstuvwxyz0123456789'.split(''),
      ...commonKeywords,
      ...beautyFashionKeywords,
    ];
    for (const q of queries) {
      try {
        const res = await axios.get('https://graph.facebook.com/v24.0/search', {
          params: {
            type: 'adinterest',
            q: q,
            limit: 1000,
            access_token: process.env.SDK_FACEBOOK_ACCESS_TOKEN,
          },
        });

        const data = res.data.data || [];
        for (const interest of data) {
          await this.prisma.targetingInterest.upsert({
            where: { id: interest.id },
            create: interest,
            update: interest,
          });
        }

        console.log(`Fetched ${data.length} interests for query: ${q}`);
      } catch {
        // nếu có lỗi thì bỏ qua và tiếp tục query tiếp theo
      }

      await new Promise((r) => setTimeout(r, 300)); // delay tránh rate-limit
    }
    return true;
  }

  // =====================================================
  // ENTRY POINT
  // =====================================================
  async syncAllAccount() {
    this.init();

    const me = new User('me');

    const [accounts, pages] = await Promise.all([
      me.getAdAccounts([...AD_ACCOUNT_FIELDS], { limit: LIMIT_DATA }),
      me.getAccounts(['id', 'name'], { limit: LIMIT_DATA }),
    ]);
    for (let index = 0; index < pages.length; index++) {
      const page = pages[index]?._data;
      await this.prisma.fanpage.upsert({
        where: { id: page?.id },
        create: { id: page?.id, name: page?.name },
        update: { name: page?.name },
      });
    }

    for await (const account of accounts) {
      const acc = account._data;
      const adAccount = new AdAccount(acc.id);

      const pixelCursor = await adAccount.getAdsPixels(AD_PIXEL_FIELDS);
      const pixels = await fetchAll(pixelCursor);

      await this.prisma.account.upsert({
        where: { id: acc.id },
        update: {
          name: acc.name,
          currency: acc.currency,
          timezone: acc.timezone_name,
          pages,
          pixels,
          rawPayload: acc,
          lastFetchedAt: new Date(),
        },
        create: {
          id: acc.id,
          name: acc.name,
          accountType: 'AD_ACCOUNT',
          currency: acc.currency,
          timezone: acc.timezone_name,
          pixels,
          pages,

          rawPayload: acc,
          lastFetchedAt: new Date(),
        },
      });

      await sleep(5000);
    }
    return { success: true, accounts };
  }
}
