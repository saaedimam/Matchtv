#!/usr/bin/env python3
"""
Matchtv exporter: clone public IPTV GitHub repos, extract playlist files
(.m3u/.m3u8/.pls + related), checksum them, optionally HEAD-check a sample of
stream URLs, and write manifest + report per repo.

Usage:
    python tools/export_agent.py --repos "iptv-org/iptv,Guovin/iptv-api" --output-dir exports --max-workers 8

Env:
    GITHUB_TOKEN (optional) - GitHub token/PAT for higher API rate limits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

import requests
from github import Github

M3U_EXTENSIONS = {'.m3u', '.m3u8', '.pls'}
USER_AGENT = "matchtv-exporter/1.0(+https://github.com/saaedimam/Matchtv)"
DEFAULT_HEAD_SAMPLE = 200  # cap HEAD checks per repo to keep runner time/bandwidth bounded

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('matchtv')


def run_cmd(cmd: List[str], cwd: Path | None = None, check: bool = True, capture: bool = False):
    logger.debug('CMD: %s (cwd=%s)', ' '.join(cmd), cwd)
    res = subprocess.run(
        cmd, cwd=cwd, check=False,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )
    if check and res.returncode != 0:
        logger.error('Command failed: %s\nstdout: %s\nstderr: %s',
                     cmd, res.stdout if capture else '<no-capture>', res.stderr if capture else '<no-capture>')
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return res


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def find_playlist_files(root: Path) -> List[Path]:
    files = []
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        if p.suffix.lower() in M3U_EXTENSIONS:
            files.append(p)
            continue
        name = p.name.lower()
        if 'playlist' in name or name.startswith('playlists') or 'channels' in name:
            files.append(p)
    return files


def parse_m3u_for_urls(path: Path) -> List[str]:
    urls = []
    try:
        with path.open('r', errors='ignore') as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith('#') and line.startswith('http'):
                    urls.append(line)
    except Exception as e:
        logger.debug('Failed parse %s: %s', path, e)
    return urls


def head_request(url: str, timeout: int = 8) -> Dict[str, Any]:
    headers = {'User-Agent': USER_AGENT}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        return {
            'url': url,
            'status': r.status_code,
            'content_type': r.headers.get('Content-Type'),
            'content_length': r.headers.get('Content-Length'),
            'elapsed': r.elapsed.total_seconds(),
        }
    except Exception as e:
        return {'url': url, 'error': str(e)}


def clone_repo(repo_full: str, work_dir: Path) -> None:
    """Shallow clone (disk-light for multi-GB repos on CI runners)."""
    if work_dir.exists():
        logger.info('Worktree exists %s', work_dir)
        return
    work_dir.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(['git', 'clone', '--depth', '1', f'https://github.com/{repo_full}.git', str(work_dir)])
    logger.info('Cloned %s -> %s', repo_full, work_dir)


def download_raw_url(url: str, dest: Path, timeout: int = 12) -> Dict[str, Any]:
    headers = {'User-Agent': USER_AGENT}
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers, stream=True)
        if r.status_code != 200:
            return {'url': url, 'status': r.status_code}
        with dest.open('wb') as fh:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        return {'url': url, 'status': 200, 'saved_to': str(dest)}
    except Exception as e:
        return {'url': url, 'error': str(e)}


def export_repo(
    repo_full: str,
    output_root: Path,
    gh: Github | None = None,
    max_workers: int = 8,
    do_head_checks: bool = True,
    head_sample: int = DEFAULT_HEAD_SAMPLE,
    keep_git: bool = False,
) -> Dict[str, Any]:
    owner, name = repo_full.split('/')
    repo_out = output_root / owner / name
    repo_out.mkdir(parents=True, exist_ok=True)
    (repo_out / 'logs').mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        'owner': owner,
        'repo': name,
        'repo_full': repo_full,
        'cloned_at': datetime.now(timezone.utc).isoformat(),
        'playlists': [],
        'streams': [],
    }

    work_dir = repo_out / 'worktree'
    try:
        clone_repo(repo_full, work_dir)
    except Exception as e:
        manifest['clone_error'] = str(e)
        logger.exception('clone failed %s', repo_full)

    if work_dir.exists():
        playlist_files = find_playlist_files(work_dir)
        logger.info('Found candidate playlist/related files %s for %s', len(playlist_files), repo_full)
        for p in playlist_files:
            try:
                rel = p.relative_to(work_dir)
            except Exception:
                rel = Path(p.name)
            dst = repo_out / 'playlists' / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(p, dst)
            except Exception:
                try:
                    dst.write_bytes(p.read_bytes())
                except Exception:
                    logger.debug('Failed copy %s', p)
                    continue
            sha = sha256_file(dst)
            manifest['playlists'].append({'path': str(rel), 'sha256': sha, 'size': dst.stat().st_size})
    else:
        logger.warning('No worktree available for repo %s, skipping file scan', repo_full)

    if work_dir.exists() and not keep_git:
        logger.info('Removing worktree clone %s (disk cleanup)', work_dir)
        shutil.rmtree(work_dir, ignore_errors=True)

    # iptv-org/iptv publishes ready-made GitHub Pages playlists
    if repo_full.lower() == 'iptv-org/iptv':
        base = 'https://iptv-org.github.io/iptv'
        for url in (f'{base}/index.m3u', f'{base}/index.category.m3u',
                    f'{base}/index.language.m3u', f'{base}/index.country.m3u'):
            dest = repo_out / 'playlists' / 'remote' / url.split('/')[-1]
            out = download_raw_url(url, dest)
            manifest.setdefault('remote_playlists', []).append(out)
            if out.get('status') == 200:
                try:
                    manifest['playlists'].append({
                        'path': f"remote/{url.split('/')[-1]}",
                        'sha256': sha256_file(dest),
                        'size': dest.stat().st_size,
                        'source_url': url,
                    })
                except Exception:
                    pass

    # Collect stream URLs from extracted playlists
    stream_urls: Set[str] = set()
    for p in (repo_out / 'playlists').rglob('*'):
        if p.is_file() and p.suffix.lower() in M3U_EXTENSIONS:
            stream_urls.update(parse_m3u_for_urls(p))
    manifest['stream_count'] = len(stream_urls)
    logger.info('Discovered unique stream URLs %s for %s', len(stream_urls), repo_full)

    # HEAD-check a bounded sample
    stream_checks: List[Dict[str, Any]] = []
    if do_head_checks and stream_urls:
        sample = sorted(stream_urls)[:head_sample]
        logger.info('HEAD-checking sample of %s stream URLs (%s workers)', len(sample), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(head_request, url): url for url in sample}
            for fut in as_completed(futures):
                try:
                    stream_checks.append(fut.result())
                except Exception as e:
                    stream_checks.append({'url': futures[fut], 'error': str(e)})
        manifest['streams'] = stream_checks
        manifest['streams_checked'] = len(stream_checks)
        manifest['streams_alive'] = sum(
            1 for s in stream_checks
            if s.get('status') is not None and 200 <= int(s.get('status')) < 400
        )
    else:
        manifest['streams'] = stream_checks

    try:
        with (repo_out / 'manifest.json').open('w') as fh:
            json.dump(manifest, fh, indent=2)
        with (repo_out / 'report.txt').open('w') as fh:
            fh.write(f"Repository: {repo_full}\n")
            fh.write(f"Cloned at: {manifest['cloned_at']}\n")
            fh.write(f"Playlists found: {len(manifest.get('playlists', []))}\n")
            fh.write(f"Streams discovered: {manifest.get('stream_count', 0)}\n")
            fh.write(f"Streams HEAD-checked: {manifest.get('streams_checked', 0)}\n")
            fh.write(f"Streams alive (2xx/3xx): {manifest.get('streams_alive', 0)}\n")
    except Exception:
        logger.exception('Failed to write manifest/report %s', repo_full)

    try:
        archive = shutil.make_archive(str(repo_out), 'gztar', root_dir=str(repo_out))
        manifest['archive'] = archive
    except Exception:
        logger.debug('Failed archive %s', repo_full)

    return manifest


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Export IPTV GitHub repositories')
    parser.add_argument('--repos', required=True, help='Comma-separated owner/repo list')
    parser.add_argument('--output-dir', default='exports', help='Output root directory')
    parser.add_argument('--max-workers', type=int, default=8, help='Max workers for HTTP checks')
    parser.add_argument('--head-sample', type=int, default=DEFAULT_HEAD_SAMPLE,
                        help='Max stream URLs to HEAD-check per repo')
    parser.add_argument('--no-head', action='store_true', help='Disable HEAD checks')
    parser.add_argument('--keep-git', action='store_true', help='Keep worktree clones after extract')
    args = parser.parse_args(argv)

    gh_token = os.getenv('GITHUB_TOKEN') or os.getenv('GITHUB_PAT')
    gh = Github(gh_token) if gh_token else None

    output_root = Path(args.output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    index = []
    for r in [r.strip() for r in args.repos.split(',') if r.strip()]:
        try:
            logger.info('Processing %s', r)
            manifest = export_repo(
                r, output_root, gh=gh,
                max_workers=args.max_workers,
                do_head_checks=(not args.no_head),
                head_sample=args.head_sample,
                keep_git=args.keep_git,
            )
            index.append({'repo': r, 'manifest': str(output_root / r.replace('/', os.sep) / 'manifest.json')})
        except Exception as e:
            logger.exception('Error processing %s', r)
            index.append({'repo': r, 'error': str(e)})

    with (output_root / 'index.json').open('w') as fh:
        json.dump(index, fh, indent=2)

    logger.info('Export complete. Output written to %s', output_root)
    return 0


if __name__ == '__main__':
    sys.exit(main())
