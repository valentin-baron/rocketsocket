use rocketsocket::framework::MessageCreate;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[rocketsocket::event]
async fn no_context(_event: MessageCreate) -> Result<(), Error> {
    Ok(())
}

fn main() {}
