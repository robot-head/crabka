#[tokio::test]
#[ignore = "requires CRABKA_ADMIN_UI_E2E_URL and installed Playwright browsers"]
async fn login_page_renders() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = std::env::var("CRABKA_ADMIN_UI_E2E_URL")?;

    let playwright = playwright_rs::Playwright::launch().await?;
    let browser = playwright.chromium().launch().await?;
    let page = browser.new_page().await?;

    page.goto(&format!("{base_url}/login"), None).await?;
    let title = page.locator("text=Sign in to Crabka").await;

    assert!(title.count().await? >= 1);

    browser.close().await?;

    Ok(())
}
