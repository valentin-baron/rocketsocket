use rocketsocket::framework::{Context, MessageCreate};

#[rocketsocket::event]
async fn no_return_type(_context: Context<()>, _event: MessageCreate) {}

fn main() {}
