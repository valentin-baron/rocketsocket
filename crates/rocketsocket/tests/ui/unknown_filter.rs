use rocketsocket::framework::{Context, MessageCreate};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[rocketsocket::event(only_admins)]
async fn unknown_filter(_context: Context<()>, _event: MessageCreate) -> Result<(), Error> {
    Ok(())
}

fn main() {}
