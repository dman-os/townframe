#!/usr/bin/env -S deno run --allow-all

// FIXME: provide a devshell for building onnxcore (python and so on)

import { $ } from "./utils.ts";
import { walk } from "jsr:@std/fs@1.0.23/walk";

async function removeTreeIfExists(targetPath: string) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await Deno.remove(targetPath, { recursive: true });
      return;
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) {
        return;
      }
      const message = error instanceof Error ? error.message : String(error);
      const isRetryable = message.includes("Directory not empty") ||
        message.includes("resource busy");
      if (!isRetryable || attempt === 4) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 200 * (attempt + 1)));
    }
  }
}

async function cleanupOrtBuildArtifacts(sourceDir: {
  join(path: string): { toString(): string };
}) {
  // Keep extracted ORT sources for reuse, but drop heavy generated Android build output.
  const androidBuildPath = `${sourceDir.join("build").toString()}/Android`;
  await removeTreeIfExists(androidBuildPath);
}

function requireEnv(name: string): string {
  const value = $.env[name];
  if (!value) {
    throw new Error(`${name} must be set for release builds`);
  }
  return value;
}

const abiToTriple = {
  "arm64-v8a": "aarch64-linux-android",
  "armeabi-v7a": "armv7-linux-androideabi",
  x86_64: "x86_64-linux-android",
  x86: "i686-linux-android",
} as const;

const abi = $.env.DAYBOOK_ANDROID_ABI ?? "arm64-v8a";
const triple = abiToTriple[abi as keyof typeof abiToTriple];
if (!triple) throw new Error(`Unsupported DAYBOOK_ANDROID_ABI=${abi}`);

const composeProfile = ($.env.DAYBOOK_COMPOSE_PROFILE ?? "debug").toLowerCase();
if (!(composeProfile === "debug" || composeProfile === "release")) {
  throw new Error(
    `Unsupported DAYBOOK_COMPOSE_PROFILE=${composeProfile}; expected debug or release`,
  );
}
const gradleTask = composeProfile === "release"
  ? "bundleRelease"
  : "assembleDebug";
const ortBuildConfig = $.env.ORT_BUILD_CONFIG ??
  (composeProfile === "release" ? "Release" : "Debug");
const androidBuildToolsVersion = $.env.ANDROID_BUILD_TOOLS_VERSION ??
  "36.0.0";
const androidSdkRoot = $.env.ANDROID_SDK_ROOT ?? $.env.ANDROID_HOME;
if (!androidSdkRoot) {
  throw new Error("ANDROID_SDK_ROOT or ANDROID_HOME must be set");
}

const ortSourceTag = $.env.ORT_SOURCE_TAG ?? "v1.24.1";
const androidApiLevel = $.env.ANDROID_API_LEVEL ?? "31";
const androidNdkRoot = $.env.ANDROID_NDK_ROOT;
if (!androidNdkRoot) throw new Error("ANDROID_NDK_ROOT must be set");
const ndkRevision = $.env.ANDROID_NDK_REVISION ??
  androidNdkRoot
    .split(/[\\/]/)
    .filter((part) => part.length > 0)
    .at(-1) ??
  "unknown-ndk";
const buildKeySuffix = `api${androidApiLevel}-ndk${ndkRevision}`.replaceAll(
  /[^\w.-]+/g,
  "_",
);

const ortRootDir = $.relativeDir("../target/ort");
const sourceArchivePath = ortRootDir.join(`onnxruntime-${ortSourceTag}.tar.gz`);
const sourceDir = ortRootDir.join(`onnxruntime-src-${ortSourceTag}`);
const sourceCompleteFile = ortRootDir.join(`.source-${ortSourceTag}.complete`);
const fetchcontentCacheDir = ortRootDir.join(
  "fetchcontent-cache",
  ortSourceTag,
);
const distDir = ortRootDir.join(
  "dist",
  ortSourceTag,
  triple,
  ortBuildConfig,
  buildKeySuffix,
);
const distCompleteFile = ortRootDir.join(
  `.dist-${ortSourceTag}-${triple}-${ortBuildConfig.toLowerCase()}-${buildKeySuffix}.complete`,
);
const libDirFile = ortRootDir.join(
  `ort-lib-location-${ortSourceTag}-${triple}-${ortBuildConfig.toLowerCase()}-${buildKeySuffix}.txt`,
);
const composeAppBuildDir = $.relativeDir(
  "../src/daybook_compose/composeApp/build",
);
const composeOutputsDir = composeAppBuildDir.join("outputs");
const releaseBundleDir = composeOutputsDir.join("bundle", "release");
const releaseApksDir = composeOutputsDir.join("apk_from_bundle", "release");
const releaseApkDir = composeOutputsDir.join("apk", "release");
const releaseBundlePath = releaseBundleDir.join("composeApp-release.aab");
const releaseApksPath = releaseApksDir.join("composeApp-release.apks");
const releaseApkPath = releaseApkDir.join("composeApp-release.apk");
const bundletoolExtractDir = composeOutputsDir.join(
  "apk_from_bundle",
  "release",
  "composeApp-release-apks",
);
const apksignerPath =
  `${androidSdkRoot}/build-tools/${androidBuildToolsVersion}/apksigner`;

await ortRootDir.ensureDir();
await fetchcontentCacheDir.ensureDir();

if (!((await distCompleteFile.exists()) && (await libDirFile.exists()))) {
  const needsSourceExtract = !(await sourceDir.exists());
  if (!(await sourceCompleteFile.exists()) || needsSourceExtract) {
    if (!(await sourceArchivePath.exists())) {
      await $.request(
        `https://github.com/microsoft/onnxruntime/archive/refs/tags/${ortSourceTag}.tar.gz`,
      )
        .showProgress()
        .pipeToPath(sourceArchivePath);
    }
    if (needsSourceExtract && (await sourceCompleteFile.exists())) {
      await Deno.remove(sourceCompleteFile.toString());
    }
    await removeTreeIfExists(sourceDir.toString());
    await sourceDir.ensureDir();
    await $`bsdtar --extract --file ${sourceArchivePath} --directory ${sourceDir} --strip-components=1`;
    await sourceCompleteFile.writeText("ok\n");
  }

  await $`bash ./build.sh --update --build --config ${ortBuildConfig} --parallel --compile_no_warning_as_error --skip_submodule_sync --build_shared_lib --android --android_abi=${abi} --android_api=${androidApiLevel} --android_ndk_path=${androidNdkRoot} --cmake_extra_defines FETCHCONTENT_BASE_DIR=${fetchcontentCacheDir} onnxruntime_BUILD_UNIT_TESTS=OFF`
    .cwd(
      sourceDir,
    );
  const builtLibDir = sourceDir.join("build", "Android", ortBuildConfig);
  const sharedLibPaths: string[] = [];
  for await (
    const entry of walk(builtLibDir.toString(), {
      includeDirs: false,
      followSymlinks: false,
    })
  ) {
    if (!entry.isFile) continue;
    if (!entry.name.includes(".so")) continue;
    sharedLibPaths.push(entry.path);
  }
  if (sharedLibPaths.length === 0) {
    throw new Error(
      `ORT build did not produce shared libraries under ${builtLibDir}`,
    );
  }
  await removeTreeIfExists(distDir.toString());
  await distDir.ensureDir();
  for (const sourceLibPath of sharedLibPaths) {
    await $`cp ${sourceLibPath} ${distDir}`;
  }
  await libDirFile.writeText(`${distDir}\n`);
  await distCompleteFile.writeText("ok\n");
}

if (
  (await sourceDir.exists()) &&
  $.env.DAYBOOK_CLEAN_ORT_ANDROID_BUILD !== "0"
) {
  // Default cleanup keeps disk use low. Set DAYBOOK_CLEAN_ORT_ANDROID_BUILD=0 to keep intermediates.
  await cleanupOrtBuildArtifacts(sourceDir);
}

await $`./gradlew ${gradleTask} -PdaybookProfile=${composeProfile} -PortLibLocation=${
  (
    await libDirFile.readText()
  ).trim()
} -PortLibProfile=${
  $.env.ORT_LIB_PROFILE ?? ortBuildConfig
} -PortPreferDynamicLink=${$.env.ORT_PREFER_DYNAMIC_LINK ?? "1"}`
  .cwd($.relativeDir("../src/daybook_compose/"))
  .env({
    ORT_LIB_LOCATION: (await libDirFile.readText()).trim(),
    ORT_LIB_PROFILE: $.env.ORT_LIB_PROFILE ?? ortBuildConfig,
    ORT_PREFER_DYNAMIC_LINK: $.env.ORT_PREFER_DYNAMIC_LINK ?? "1",
  });

if (composeProfile === "release") {
  const keystorePath = requireEnv("DAYBOOK_ANDROID_RELEASE_KEYSTORE_PATH");
  const keystorePassword = requireEnv(
    "DAYBOOK_ANDROID_RELEASE_KEYSTORE_PASSWORD",
  );
  const keyAlias = requireEnv("DAYBOOK_ANDROID_RELEASE_KEY_ALIAS");
  const keyPassword = requireEnv("DAYBOOK_ANDROID_RELEASE_KEY_PASSWORD");
  const bundletoolSecretsDir = await Deno.makeTempDir({
    prefix: "daybook-bundletool-secrets-",
  });
  const keystorePasswordFile = `${bundletoolSecretsDir}/ks-pass.txt`;
  const keyPasswordFile = `${bundletoolSecretsDir}/key-pass.txt`;

  await releaseApksDir.ensureDir();
  await releaseApkDir.ensureDir();
  await removeTreeIfExists(releaseApksPath.toString());
  await removeTreeIfExists(releaseApkPath.toString());
  await removeTreeIfExists(bundletoolExtractDir.toString());
  await bundletoolExtractDir.ensureDir();

  try {
    await Deno.writeTextFile(keystorePasswordFile, keystorePassword);
    await Deno.chmod(keystorePasswordFile, 0o600);
    await Deno.writeTextFile(keyPasswordFile, keyPassword);
    await Deno.chmod(keyPasswordFile, 0o600);

    await $`bundletool build-apks --bundle=${releaseBundlePath} --output=${releaseApksPath} --mode=universal --overwrite --ks=${keystorePath} --ks-pass=file:${keystorePasswordFile} --ks-key-alias=${keyAlias} --key-pass=file:${keyPasswordFile}`;
    const universalApkPath = bundletoolExtractDir.join("universal.apk");
    await $`bsdtar --extract --file ${releaseApksPath} --directory ${bundletoolExtractDir}`;
    if (!(await universalApkPath.exists())) {
      throw new Error(
        `bundletool did not produce universal APK at ${universalApkPath}`,
      );
    }
    await $`cp ${universalApkPath} ${releaseApkPath}`;
    await $`${apksignerPath} verify --verbose --print-certs ${releaseApkPath}`;
  } finally {
    await removeTreeIfExists(bundletoolSecretsDir);
    await removeTreeIfExists(bundletoolExtractDir.toString());
  }
}
