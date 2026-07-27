import { getEnvironment } from "wasi:cli/environment@0.2.6";

export function getConfig() {
  return {
    $WORKER_ID: "btress_auth_prime",
    $BTRESS_URL: getEnv("BTRESS_URL"),
    $ROOT_WEB_DOMAIN: getEnv("ROOT_WEB_DOMAIN"),

    // $PG_URL: getEnv("PG_URL"),

    $BETTER_AUTH_URL: getEnv("BETTER_AUTH_URL"),
    $BETTER_AUTH_SECRET: getEnv("BETTER_AUTH_SECRET"),

    $SMTP_URL: getEnv("SMTP_URL"),
    $EMAIL_SUPPORT_ADDR: getEnv("EMAIL_SUPPORT_ADDR"),
    $EMAIL_SUPPORT_NAME: getEnv("EMAIL_SUPPORT_NAME"),
    $EMAIL_BOT_ADDR: getEnv("EMAIL_BOT_ADDR"),
    $EMAIL_BOT_NAME: getEnv("EMAIL_BOT_NAME"),

    $S3_REGION: getEnv("S3_REGION"),
    $S3_BUCKET: getEnv("S3_BUCKET"),
    $S3_ACCESS_KEY_ID: getEnv("S3_ACCESS_KEY_ID"),
    $S3_SECRET_ACCESS_KEY: getEnv("S3_ACCESS_KEY_SECRET"),
    $S3_ENDPOINT: getEnvOptional("S3_ENDPOINT"),
    $S3_FORCE_PATH_STYLE:
      (getEnvOptional("S3_FORCE_PATH_STYLE") ?? "false") === "true",
  };
}

const allEnvs = Object.fromEntries(getEnvironment());
export function getEnvOptional(key: string) {
  return allEnvs[key];
}

export function getEnv(key: string) {
  return assertNotNull(allEnvs[key], key);
}

export function assertNotNull<T>(value: T | undefined | null, cx?: string) {
  if (value === undefined) {
    throw Error(`value undefined${cx ? `: ${cx}` : ""}`);
  }
  if (value === null) {
    throw Error(`value null${cx ? `: ${cx}` : ""}`);
  }
  return value;
}
