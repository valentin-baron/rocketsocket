use rocketsocket::framework::{Context, MessageCreate};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[rocketsocket::event(MessageCreate)]
async fn takes_arguments(_context: Context<()>, _event: MessageCreate) -> Result<(), Error> {
    Ok(())
}

fn main() {}
