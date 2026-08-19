use rocketsocket::framework::{Context, MessageCreate};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[rocketsocket::event]
fn not_async(_context: Context<()>, _event: MessageCreate) -> Result<(), Error> {
    Ok(())
}

fn main() {}
