#!/usr/bin/env node
import { Command } from "commander";
import { globby } from "globby";
import xxhash from "xxhash-wasm";
import fs from "fs/promises";
import path from "path";
import os from "os";
import pLimit from "p-limit";

const program = new Command();

program
    .name("justnew")
    .description("Delete duplicate files from <newDir> that also exist (by content) in <oldDir>")
    .argument("<oldDir>", "reference directory")
    .argument("<newDir>", "directory to clean")
    .option("-d, --dry-run", "only show what would be deleted")
    .option("-v, --verbose", "log progress and status to stderr")
    .option("-n, --match-names", "use [filename, size] as preliminary match key")
    .option("-p, --match-paths", "use [relative path, size] as preliminary match key")
    .option("--concurrency <n>", "number of parallel operations", value => parseInt(value, 10))
    .parse();

const options = program.opts();
const [oldDirInput, newDirInput] = program.args;

if (!oldDirInput || !newDirInput) {
    console.error("Error: Both oldDir and newDir are required.");
    process.exit(1);
}

const verbose = !!options["verbose"];
const dryRun = !!options["dryRun"];
const matchNames = !!options["matchNames"];
const matchPaths = !!options["matchPaths"];
const concurrency = options["concurrency"] ?? os.cpus().length;
const limit = pLimit(concurrency);

function logStage(message: string): void {
    if (verbose) console.error(message);
}

type Phase1Key = string;
type Phase1Map = Map<Phase1Key, string[]>;
type FileHashMap = Map<string, string[]>; // (preKey + : + hash) → files

function computePreliminaryKey(absPath: string, baseDir: string, size: number): string {
    if (matchPaths) {
        const rel = path.relative(baseDir, absPath);
        return JSON.stringify([rel, size]);
    } else if (matchNames) {
        return JSON.stringify([path.basename(absPath), size]);
    } else {
        return size.toString();
    }
}

async function getPreliminaryMap(dir: string, label: string): Promise<Phase1Map> {
    const files = await globby(["**/*"], { cwd: dir, absolute: true, onlyFiles: true });
    const map: Phase1Map = new Map();
    let processed = 0;

    logStage(`Grouping files in ${label} by preliminary key (${files.length} files)`);

    await Promise.all(
        files.map(file =>
            limit(async () => {
                try {
                    const stat = await fs.stat(file);
                    const key = computePreliminaryKey(file, dir, stat.size);
                    if (!map.has(key)) map.set(key, []);
                    map.get(key)!.push(file);
                } catch (err) {
                    console.error(`Error stating file: ${file}`, err);
                }

                if (verbose) {
                    processed++;
                    if (processed % 100 === 0 || processed === files.length) {
                        console.error(`[${label}] ${processed}/${files.length} grouped`);
                    }
                }
            })
        )
    );

    logStage(`Finished grouping ${label} (${map.size} key buckets)`);
    return map;
}

function hashToHex(hashBigInt: bigint): string {
    return hashBigInt.toString(16).padStart(16, "0");
}

async function hashGroup(files: string[], preKey: string, label: string, hasher: (data: Uint8Array) => bigint): Promise<FileHashMap> {
    const map: FileHashMap = new Map();
    let processed = 0;

    await Promise.all(
        files.map(file =>
            limit(async () => {
                try {
                    const buf = await fs.readFile(file);
                    const hash = hashToHex(hasher(buf));
                    const key = `${preKey}:${hash}`;
                    if (!map.has(key)) map.set(key, []);
                    map.get(key)!.push(file);
                } catch (err) {
                    console.error(`Error hashing file: ${file}`, err);
                }

                if (verbose) {
                    processed++;
                    if (processed % 100 === 0 || processed === files.length) {
                        console.error(`[${label}] ${processed}/${files.length} hashed`);
                    }
                }
            })
        )
    );

    return map;
}

async function dedupe(oldDir: string, newDir: string): Promise<void> {
    logStage(`Using concurrency: ${concurrency}`);
    if (dryRun) logStage(`Dry run enabled — no files will be deleted.`);

    const hasher = await xxhash();

    const [oldGroups, newGroups] = await Promise.all([
        getPreliminaryMap(oldDir, "old directory"),
        getPreliminaryMap(newDir, "new directory"),
    ]);

    const intersectingKeys = [...oldGroups.keys()].filter(key => newGroups.has(key));
    logStage(`Found ${intersectingKeys.length} overlapping buckets`);

    const oldHashMap: FileHashMap = new Map();
    const newHashMap: FileHashMap = new Map();

    for (const key of intersectingKeys) {
        const oldGroup = oldGroups.get(key)!;
        const newGroup = newGroups.get(key)!;

        const [oldHashes, newHashes] = await Promise.all([
            hashGroup(oldGroup, key, `old:${key}`, data => hasher.h64Raw(data)),
            hashGroup(newGroup, key, `new:${key}`, data => hasher.h64Raw(data)),
        ]);

        for (const [hkey, paths] of oldHashes) {
            if (!oldHashMap.has(hkey)) oldHashMap.set(hkey, []);
            oldHashMap.get(hkey)!.push(...paths);
        }

        for (const [hkey, paths] of newHashes) {
            if (!newHashMap.has(hkey)) newHashMap.set(hkey, []);
            newHashMap.get(hkey)!.push(...paths);
        }
    }

    const duplicateKeys = [...oldHashMap.keys()].filter(k => newHashMap.has(k));
    duplicateKeys.sort((a, b) => oldHashMap.get(a)![0].localeCompare(oldHashMap.get(b)![0]));

    for (const key of duplicateKeys) {
        const oldFiles = oldHashMap.get(key)!;
        const newFiles = newHashMap.get(key)!;

        oldFiles.sort();
        newFiles.sort();

        for (const file of oldFiles) {
            console.log(file);
        }
        for (const file of newFiles) {
            console.log("  " + file);
        }
        console.log();

        for (const newFile of newFiles) {
            if (!dryRun) {
                try {
                    await fs.unlink(newFile);
                    if (!verbose) console.log(`Deleted: ${newFile}`);
                } catch (err) {
                    console.error(`Error deleting ${newFile}:`, err);
                }
            }
        }
    }

    if (!verbose) {
        console.log("Done.");
    }
}

await dedupe(path.resolve(oldDirInput), path.resolve(newDirInput)).catch(err => {
    console.error("Fatal error:", err);
    process.exit(1);
});
