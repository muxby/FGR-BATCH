#!/usr/bin/env python3
"""
Forza Horizon 5 - Queue-Based Parallel Downloader
Reads from url.txt, clicks 'Continue to Download' -> 'Start Download' (Waits 6s) -> Clicks Again.

REQUIREMENTS:
    pip install playwright rich
    playwright install chromium
"""

import os
import sys
import asyncio
import argparse
import shutil
from pathlib import Path

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("ERROR: Install playwright: pip install playwright && playwright install chromium")
    sys.exit(1)

try:
    from rich.console import Console
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

def log(msg: str, color: str = "white"):
    """Print colored log message"""
    if HAS_RICH:
        console = Console()
        console.print(f"[{color}]{msg}[/{color}]")
    else:
        print(msg)


class Downloader:
    def __init__(self, urls: list, output_dir: str, workers: int = 5, headless: bool = False):
        self.urls = urls
        self.total_urls = len(urls)
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.workers = workers
        self.headless = headless
        
        self.downloaded = 0
        self.failed = 0
        self.failed_urls = []

        self.queue = asyncio.Queue()
        for i, url in enumerate(self.urls, 1):
            self.queue.put_nowait((i, url))

        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def click_text(self, page, text: str) -> bool:
        """Injects JS to click buttons, strictly avoiding 'Premium' buttons"""
        try:
            result = await page.evaluate('''
                (text) => {
                    text = text.toLowerCase();
                    
                    // Filter out sneaky premium and ad buttons
                    const badWords = ['premium', 'fast', 'vip', 'torrent', 'high speed'];
                    const isSafe = (t) => !badWords.some(bad => t.includes(bad));
                    
                    // 1. Check Links and Buttons
                    const elements = document.querySelectorAll('a, button, input[type="button"], input[type="submit"]');
                    for (let e of elements) {
                        const t = (e.innerText || e.textContent || e.value || '').trim().toLowerCase();
                        if (t && t.includes(text) && isSafe(t)) {
                            e.click();
                            return true;
                        }
                    }
                    
                    // 2. Check strict text inside generic boxes (Divs/Spans)
                    const boxes = document.querySelectorAll('div, span');
                    for (let e of boxes) {
                        const t = (e.innerText || e.textContent || '').trim().toLowerCase();
                        if (t && (t === text || t === 'continue to download' || t === 'start download' || t === 'free download' || t === 'download file') && isSafe(t)) {
                            e.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''', text)
            
            if result:
                await asyncio.sleep(4) # Allow time for the click to trigger the next page/popup
            return result
        except:
            return False

    async def _attempt_download(self, context, page, url: str, index: int):
        filename = url.split('/')[-1]
        filepath = self.output_dir / filename
        log_prefix = f"[Task {index}/{self.total_urls}] {filename}"

        if filepath.exists() and filepath.stat().st_size > 100 * 1024 * 1024:
            log(f"  [SKIP] {log_prefix} already exists", "yellow")
            self.downloaded += 1
            return

        log(f"  [STARTING] {log_prefix}")

        download_future = asyncio.Future()
        
        def handle_download(d):
            # Only resolve the future for the very first file that starts downloading
            if not download_future.done():
                download_future.set_result(d)

        def handle_new_page(new_page):
            # Listen to any popups or new tabs for the actual file drop
            new_page.on('download', handle_download)

        context.on('page', handle_new_page)
        page.on('download', handle_download)

        # 1. Load initial URL
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await asyncio.sleep(8) 

        # 2. Click "Continue to Download"
        log(f"    -> Clicking 'Continue to Download'", "cyan")
        await self.click_text(page, 'continue to download')
        
        # 3. Handle popup tabs (if site opened the download page in a new tab)
        active_page = context.pages[-1]
        
        # 4. Click "Start Download" (First time)
        log(f"    -> Clicking 'Start Download' (Click 1)", "cyan")
        clicked = await self.click_text(active_page, 'start download')
        if not clicked:
            clicked = await self.click_text(active_page, 'free download')
        if not clicked:
            await self.click_text(active_page, 'download')

        # --- NEW LOGIC: Wait 6 seconds, then click again ---
        log(f"    -> Waiting 6 seconds...", "dim")
        await asyncio.sleep(6)

        log(f"    -> Clicking 'Start Download' again (Click 2)", "cyan")
        clicked_again = await self.click_text(active_page, 'start download')
        if not clicked_again:
            clicked_again = await self.click_text(active_page, 'free download')
        if not clicked_again:
            await self.click_text(active_page, 'download')
        # ---------------------------------------------------

        # 5. Wait for the file to actually drop into your folder
        try:
            download = await asyncio.wait_for(download_future, timeout=120)
            download_path = await download.path()

            if download_path and Path(download_path).exists():
                shutil.copy2(str(download_path), str(filepath))
                
                if filepath.exists():
                    size_mb = filepath.stat().st_size / (1024 * 1024)
                    log(f"    [OK] {log_prefix} ({size_mb:.0f} MB)", "green")
                    self.downloaded += 1
                    return
        except asyncio.TimeoutError:
            raise Exception("Download timeout. Site may have blocked the request, required a captcha, or the button sequence failed.")

        raise Exception("File was not saved successfully.")

    async def worker(self, browser, worker_id: int):
        context = await browser.new_context(
            accept_downloads=True,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        )
        
        await context.add_init_script('''
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        ''')

        while True:
            try:
                index, url = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break 

            page = await context.new_page()
            page.set_default_timeout(60000)

            try:
                await self._attempt_download(context, page, url, index)
            except Exception as e:
                log(f"  [ERROR] Task {index} failed: {str(e)[:80]}", "red")
                self.failed += 1
                self.failed_urls.append(url)
            finally:
                for p in context.pages:
                    try:
                        await p.close()
                    except:
                        pass
            
            self.queue.task_done()
            await asyncio.sleep(2) 

        await context.close()

    async def run(self):
        log(f"\n{'='*60}", "cyan")
        log(f"Queue-Based Downloader | Workers: {self.workers}", "bold cyan")
        log(f"Output: {self.output_dir}", "cyan")
        log(f"Files to process: {self.total_urls}", "cyan")
        log(f"{'='*60}\n", "cyan")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )

            tasks = [
                asyncio.create_task(self.worker(browser, i))
                for i in range(self.workers)
            ]
            
            await self.queue.join()
            await browser.close()

        if self.failed_urls:
            failed_file = self.output_dir / "failed_urls.txt"
            with open(failed_file, 'w') as f:
                for url in self.failed_urls:
                    f.write(url + '\n')

        log(f"\n{'='*60}", "cyan")
        log("DOWNLOAD COMPLETE", "bold cyan")
        log(f"Completed: {self.downloaded} | Failed: {self.failed}", "green" if self.failed == 0 else "yellow")
        if self.failed_urls:
            log(f"Failed URLs saved to: {self.output_dir / 'failed_urls.txt'}", "red")
        log(f"{'='*60}\n", "cyan")


def main():
    parser = argparse.ArgumentParser(description='FH5 Downloader (From url.txt)')
    
    # MODIFIED LINE: Ensures default path is 'FH5_FitGirl' exactly where this script lives
    script_dir = Path(__file__).resolve().parent
    target_dir = str(script_dir / 'FH5_FitGirl')
    
    parser.add_argument('--output-dir', default=target_dir, help='Output directory')
    parser.add_argument('--workers', type=int, default=5, help='Concurrent downloads (default: 5)')
    parser.add_argument('--headless', action='store_true', help='Run without visible browser')
    args = parser.parse_args()

    url_file = Path("url.txt").resolve()
    if not url_file.exists():
        print(f"ERROR: Could not find '{url_file}'.")
        print("Please create a text file named 'url.txt' in this directory and paste your datanodes links inside it.")
        sys.exit(1)

    with open(url_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip().startswith('http')]

    if not urls:
        print("ERROR: 'url.txt' was found but contains no valid links (must start with http).")
        sys.exit(1)

    downloader = Downloader(
        urls=urls,
        output_dir=args.output_dir,
        workers=args.workers,
        headless=args.headless
    )

    try:
        asyncio.run(downloader.run())
    except KeyboardInterrupt:
        log("\nDownload manually interrupted by user!", "yellow")

if __name__ == '__main__':
    main()