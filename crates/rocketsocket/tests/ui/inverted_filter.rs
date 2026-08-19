// `not_self` is the default, so asking for it means the author has the polarity backwards
// and may believe the opposite is also available. The error has to say so, or they will
// write `#[event(not_self)]`, see it compile, and assume `#[event]` alone is unsafe.
use rocketsocket::framework::{Context, MessageCreate};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[rocketsocket::event(not_self)]
async fn inverted(_context: Context<()>, _event: MessageCreate) -> Result<(), Error> {
    Ok(())
}

fn main() {}
