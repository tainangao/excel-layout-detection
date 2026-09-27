from playwright.sync_api import sync_playwright

playwright = sync_playwright().start()
browser = playwright.chromium.launch(headless=False)
page = browser.new_page()
page.goto("https://www.playwright.dev/")
page.screenshot(path="example.png")
browser.close()

playwright.stop()