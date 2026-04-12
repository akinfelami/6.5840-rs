#[cfg(test)]
mod tests {
    use crate::util;

    #[tokio::test]
    async fn test_wc() {
        util::mk_out().await;
    }
}
