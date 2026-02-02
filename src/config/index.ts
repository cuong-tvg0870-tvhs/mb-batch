import appConfig from './app.config';
import authConfig from './auth.config';
import databaseConfig from './database.config';

export const configLoads = [databaseConfig, appConfig, authConfig];
