#!/usr/bin/env -S deno run --allow-all

// FIXME: provide a devshell for building onnxcore (python and so on)

import { $ } from "./utils.ts";

const abiToTriple = {
	"arm64-v8a": "aarch64-linux-android",
	"armeabi-v7a": "armv7-linux-androideabi",
	x86_64: "x86_64-linux-android",
	x86: "i686-linux-android",
} as const;

const abi = $.env.DAYBOOK_ANDROID_ABI ?? "arm64-v8a";
const triple = abiToTriple[abi as keyof typeof abiToTriple];
if (!triple) throw new Error(`Unsupported DAYBOOK_ANDROID_ABI=${abi}`);

const ortSourceTag = $.env.ORT_SOURCE_TAG ?? "v1.24.1";
const androidApiLevel = $.env.ANDROID_API_LEVEL ?? "31";
const androidNdkRoot = $.env.ANDROID_NDK_ROOT;
if (!androidNdkRoot) throw new Error("ANDROID_NDK_ROOT must be set");

const ortRootDir = $.relativeDir("../target/ort");
const sourceArchivePath = ortRootDir.join(`onnxruntime-${ortSourceTag}.tar.gz`);
const sourceDir = ortRootDir.join("onnxruntime-src");
const sourceCompleteFile = ortRootDir.join(`.source-${ortSourceTag}.complete`);
const buildCompleteFile = ortRootDir.join(`.build-${triple}.complete`);
const libDirFile = ortRootDir.join(`ort-lib-location-${triple}.txt`);

await ortRootDir.ensureDir();

if (!(await sourceCompleteFile.exists())) {
	await $.request(
		`https://github.com/microsoft/onnxruntime/archive/refs/tags/${ortSourceTag}.tar.gz`,
	)
		.showProgress()
		.pipeToPath(sourceArchivePath);
	await sourceDir.ensureRemove();
	await sourceDir.ensureDir();
	await $`bsdtar --extract --file ${sourceArchivePath} --directory ${sourceDir} --strip-components=1`;
	await sourceCompleteFile.writeText("ok\n");
}

if (!(await buildCompleteFile.exists())) {
	await $`bash ./build.sh --update --build --config Release --parallel --compile_no_warning_as_error --skip_submodule_sync --android --android_abi=${abi} --android_api=${androidApiLevel} --android_ndk_path=${androidNdkRoot}`.cwd(
		sourceDir,
	);
	await libDirFile.writeText(
		`${sourceDir.join("build", "Android", "Release")}\n`,
	);
	await buildCompleteFile.writeText("ok\n");
}

await $`./gradlew installDebug`
	.cwd($.relativeDir("../src/daybook_compose/"))
	.env({
		ORT_LIB_LOCATION: (await libDirFile.readText()).trim(),
		ORT_LIB_PROFILE: $.env.ORT_LIB_PROFILE ?? "Release",
	});
