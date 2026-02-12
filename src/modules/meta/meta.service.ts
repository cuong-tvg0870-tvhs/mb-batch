import { Injectable } from '@nestjs/common';
import axios from 'axios';
import 'dotenv/config';
import {
  AdAccount,
  AdVideo,
  FacebookAdsApi,
  User,
} from 'facebook-nodejs-business-sdk';
import {
  beautyFashionKeywords,
  commonKeywords,
  fetchAll,
  LIMIT_DATA,
  toPrismaJson,
} from '../../common/utils';

import { MetaAd } from 'src/common/dtos/types.dto';
import { sleep } from '../../common/utils';
import {
  AD_ACCOUNT_FIELDS,
  AD_IMAGE_FIELDS,
  AD_PIXEL_FIELDS,
  AD_VIDEO_FIELDS,
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

  async syncAdAssetsLegacy(
    adAccount: AdAccount,
    accountId: string,
    ad: MetaAd,
  ) {
    /** IMAGE */
    if (ad.creative?.image_hash) {
      const exists = await this.prisma.adImage.findFirst({
        where: { hash: ad.creative.image_hash },
      });

      if (!exists) {
        const cursor = await adAccount.getAdImages(
          AD_IMAGE_FIELDS,
          { hashes: [ad.creative.image_hash] },
          true,
        );

        const image = (await fetchAll(cursor))[0];
        if (image) {
          await this.prisma.adImage.upsert({
            where: {
              accountId_hash_id: {
                id: image.id,
                accountId,
                hash: image.hash,
              },
            },
            update: {
              name: image.name,
              url: image.permalink_url || image.url,
              permalink_url: image.permalink_url,
              height: image.height,
              width: image.width,
              rawPayload: toPrismaJson(image),
              status: image.status,
              createdTime: image.created_time
                ? new Date(image.created_time)
                : undefined,
              createdAt: image.created_time
                ? new Date(image.created_time)
                : undefined,
              updatedAt: image.updated_time
                ? new Date(image.updated_time)
                : undefined,
            },
            create: {
              id: image.id,
              accountId,
              hash: image.hash,
              name: image.name,
              url: image.permalink_url || image.url,
              permalink_url: image.permalink_url,

              height: image.height,
              width: image.width,
              rawPayload: toPrismaJson(image),
              status: image.status,
              createdTime: image.created_time
                ? new Date(image.created_time)
                : undefined,
              createdAt: image.created_time
                ? new Date(image.created_time)
                : undefined,
              updatedAt: image.updated_time
                ? new Date(image.updated_time)
                : undefined,
            },
          });
        }
      }
    }
    /** VIDEO */
    if (ad.creative?.video_id) {
      const exists = await this.prisma.adVideo.findFirst({
        where: { id: ad.creative.video_id },
      });

      if (!exists) {
        const videoId = ad.creative.video_id;

        let uploadResult: any = null;

        try {
          const videoNode = await new AdVideo(videoId).read(AD_VIDEO_FIELDS);
          uploadResult = videoNode?._data;
        } catch (err) {
          console.warn(
            `⚠️ Cannot read video ${videoId}, fallback to creative data`,
          );
        }

        /**
         * Fallback nếu không read được video
         */
        const fallbackData = {
          id: videoId,
          title: `Video ${videoId} - no permission`,
          accountId,
          thumbnailUrl: ad.creative?.thumbnail_url || null,
        };

        const finalData = uploadResult
          ? {
              id: uploadResult.id,
              title: uploadResult?.title,
              accountId,
              source:
                uploadResult.source ||
                `https://facebook.com/${uploadResult.permalink_url}`,
              status: uploadResult?.status?.video_status,
              thumbnailUrl: uploadResult?.picture || uploadResult?.source,
              length: uploadResult?.length,
              rawPayload: uploadResult,
            }
          : fallbackData;

        await this.prisma.adVideo.upsert({
          where: { id: finalData.id },
          create: finalData,
          update: finalData,
        });
      }
    }
  }
}
