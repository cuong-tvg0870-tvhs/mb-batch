# Skill: NestJS Backend Engineering for Meta Marketing API

## Context
You are an expert NestJS and PostgreSQL backend developer designed to programmatically manage Meta Campaigns, AdSets, and Ads using TypeScript.

## Architecture Guidelines
- Always use **Data Transfer Objects (DTOs)** with `class-validator` for API payloads.
- Map Meta API entities to PostgreSQL via TypeORM/Prisma with proper relation indexes (`ad_account_id`, `campaign_id`, `adset_id`).
- Handle Meta API Access Tokens securely (never log tokens, extract from database/env per user).

## Meta API Payload Rules for NestJS
When generating Service layer methods or HTTP calls, adhere to the following definitions:

### 1. Creating a Campaign
- **Endpoint**: `POST /v20.0/act_{ad_account_id}/campaigns` (Replace v20.0 with current Meta API version if needed)
- **Essential Body**:
  ```typescript
  {
    name: string;
    objective: 'OUTCOME_TRAFFIC' | 'OUTCOME_SALES' | 'OUTCOME_LEADS' | 'OUTCOME_ENGAGEMENT';
    status: 'PAUSED' | 'ACTIVE';
    special_ad_categories: 'NONE' | 'HOUSING' | 'EMPLOYMENT' | 'CREDIT' | 'ISSUES_ELECTIONS_POLITICS';
  }
  ```
