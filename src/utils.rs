pub async fn get(url: &str) -> Result<reqwest::Response, reqwest::Error> {
    let req = reqwest::get(url).await?;
    let req = req.error_for_status()?;

    Ok(req)
}
