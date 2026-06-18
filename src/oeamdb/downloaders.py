from pathlib import Path
import time
import csv
import json
import requests

from playwright.sync_api import sync_playwright



class BasgDownloader:
    def __init__(self,
        browser_type="firefox",
        page_url="https://medikamente.basg.gv.at/de/medicinal-products",
        api_url = "https://medikamente.basg.gv.at/api/api/v1/medication/search",
        page_size = 10_000,
        short_timeout_ms = 5_000,
        long_timeout_ms = 45_000,
        data_folder=Path("./basg_download"),
        filename="basg",
        headless_mode=True,
        ):
        self.page_url = page_url
        self.api_url = api_url
        self.page_size = page_size
        self.short_timeout_ms = short_timeout_ms
        self.long_timeout_ms = long_timeout_ms
        self.data_folder = Path(data_folder)
        self.filename = filename
        self.browser_type = browser_type
        self.headless_mode = headless_mode

    def download(self,csv_dl=True,json_dl=True, force=False):


        csv_path = self.data_folder / f"{self.filename}.csv"
        json_path = self.data_folder / f"{self.filename}.json"
        csv_dl_needed = csv_dl and (force or not csv_path.exists())
        json_dl_needed = json_dl and (force or not json_path.exists())

        if not csv_dl_needed and not json_dl_needed:
            return


        with sync_playwright() as p:
            browser = getattr(p,self.browser_type).launch(headless=self.headless_mode)
            context = browser.new_context()
            page = context.new_page()



            page.goto(self.page_url, wait_until="networkidle", timeout=self.short_timeout_ms)

            with page.expect_request(
                lambda r: "medication/search" in r.url and r.method == "POST",
                timeout=self.short_timeout_ms) as req_info:
                page.get_by_role("button", name="Suche", exact=True).click()

            req = req_info.value
            base_url = req.url.split("?")[0]
            body = req.post_data
            auth = req.headers.get("authorization")

            self.data_folder.mkdir(parents=True, exist_ok=True)

            if json_dl_needed:
                rows = self.fetch_all_pages(context=context, body=body, auth=auth)
                rows_en = self.fetch_all_pages(context=context, body=body, auth=auth, lang="EN")
                substances_en = {d['id']:d['substances'] for d in rows_en}
                for r in rows:
                    r['substances_en'] = substances_en.get(r['id'],None)
                with open(json_path,'w') as f:
                    f.write(json.dumps(rows, indent=4))

            if csv_dl_needed:
                page.get_by_role("button", name="Download button", exact=True).click()
                page.wait_for_timeout(self.short_timeout_ms)
                with page.expect_download(timeout=self.long_timeout_ms) as dl_info:
                    page.get_by_role("menuitem", name="Export als .csv").click()
                download = dl_info.value
                download.save_as(csv_path)

            browser.close()




    def fetch_all_pages(self, context, body, auth, lang="DE") -> list[dict]:
        rows: list[dict] = []
        page_n = 1
        while True:
            resp = context.request.post(
                f"{self.api_url}?page={page_n}&size={self.page_size}",
                data=body,
                headers={
                    "Authorization": auth,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Language": lang,
                },
                timeout=self.short_timeout_ms,
                )
            if not resp.ok:
                raise RuntimeError(f"page {page_n}: HTTP {resp.status} {resp.text()[:200]}")
    
            data = resp.json()
    
            # Spring-style pageable: items live in "content"; plain APIs may
            # return a bare list. Adjust the key if yours differs.
            items = data.get("items", data) if isinstance(data, dict) else data
            total_items = data.get("totalItems",-1)
            if not items:
                break
            rows.extend(items)
            print(f"page {page_n}: {len(items)} items (total {len(rows)}/{total_items})")
    
            if len(rows) == total_items or len(items) < self.page_size:
                break
            page_n += 1
        return rows
