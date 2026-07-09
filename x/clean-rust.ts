#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

type CargoMetadata = {
	packages: CargoPackage[];
	workspace_members: string[];
};

type CargoPackage = {
	id: string;
	name: string;
};

const metadata = await $`cargo metadata --format-version 1 --no-deps`.json();

const workspaceMemberIds = new Set(metadata.workspace_members);
const workspacePackages = metadata.packages
	.filter((pkg) => workspaceMemberIds.has(pkg.id))
	.map((pkg) => pkg.name);

if (workspacePackages.length === 0) {
	throw new Error("cargo metadata returned no workspace packages");
}

await $.raw`cargo clean ${workspacePackages.map((name) => `-p ${name}`).join(" ")}`;
