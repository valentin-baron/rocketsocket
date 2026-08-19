//! The optional handler layer.
//!
//! Everything here is sugar over the event stream. A handler is a **value** — the
//! `#[event]` macro expands to a function returning one — so registration is explicit and
//! inspectable rather than a side effect, and a bot that wants the raw stream keeps it.
//! Poise is built on serenity this way, by someone who did not own serenity, and that is
//! the property worth preserving.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketsocket_model::event::StreamEvent;
use rocketsocket_realtime::client::ClientEvent;

/// A boxed handler future.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'a>>;

/// An error escaping a handler.
///
/// Boxed rather than generic: a handler that fails should be reported and the loop should
/// continue, and threading a user error type through the registry buys nothing for that.
#[derive(Debug)]
pub struct HandlerError(Box<dyn std::error::Error + Send + Sync>);

impl HandlerError {
    /// Wraps an error returned by a handler body.
    ///
    /// Takes `Into<Box<dyn Error>>` rather than `E: Error`, because the most natural
    /// handler error type — `Box<dyn Error + Send + Sync>` — does not itself implement
    /// `Error` (the blanket impl for `Box<T>` requires `T: Sized`). Bounding on `Error`
    /// would reject exactly the signature most people write first.
    pub fn from_handler(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(error.into())
    }

    /// Builds an error for a parameter that could not be produced.
    #[must_use]
    pub fn extraction(reason: String) -> Self {
        Self(reason.into())
    }

    /// The underlying error.
    #[must_use]
    pub fn inner(&self) -> &(dyn std::error::Error + Send + Sync) {
        self.0.as_ref()
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for HandlerError {}

/// Why a handler did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Extract {
    /// This event is not the one the handler declared. Not an error.
    Skip,
    /// The event matched but a parameter could not be produced.
    Failed(String),
}

/// Everything a handler can be handed.
///
/// A handler declares only the parameters it wants, in any order, and each is produced
/// from the event by this trait — the same shape as an axum extractor.
pub trait FromEvent<D>: Sized {
    /// Produces this parameter, or explains why the handler should not run.
    ///
    /// # Errors
    /// [`Extract::Skip`] when the event is not the handler's; [`Extract::Failed`] when it
    /// is but the parameter could not be built.
    fn from_event(event: &ClientEvent, context: &Context<D>) -> Result<Self, Extract>;
}

/// Shared state and transports, handed to every handler.
///
/// Generic over the bot's own data rather than a typemap: poise abandoned serenity's
/// `TypeMap` for exactly this, and a greenfield crate has no back-compatibility reason to
/// repeat it. No global lock, no runtime `Option`, no key that might not be inserted.
#[derive(Debug)]
pub struct Context<D> {
    data: Arc<D>,
    bot: crate::Bot,
}

impl<D> Clone for Context<D> {
    fn clone(&self) -> Self {
        Self { data: Arc::clone(&self.data), bot: self.bot.clone() }
    }
}

impl<D> Context<D> {
    /// Builds a context.
    #[must_use]
    pub fn new(bot: crate::Bot, data: Arc<D>) -> Self {
        Self { data, bot }
    }

    /// The bot's own state.
    #[must_use]
    pub fn data(&self) -> &Arc<D> {
        &self.data
    }

    /// The connected bot, for REST calls and subscriptions.
    #[must_use]
    pub fn bot(&self) -> &crate::Bot {
        &self.bot
    }
}

impl<D> FromEvent<D> for Context<D> {
    fn from_event(_event: &ClientEvent, context: &Context<D>) -> Result<Self, Extract> {
        Ok(context.clone())
    }
}

/// Extractor for the bot's own state.
#[derive(Debug, Clone)]
pub struct State<D>(pub Arc<D>);

impl<D> FromEvent<D> for State<D> {
    fn from_event(_event: &ClientEvent, context: &Context<D>) -> Result<Self, Extract> {
        Ok(Self(Arc::clone(context.data())))
    }
}

/// A registered handler.
#[derive(Clone)]
pub struct Handler<D> {
    name: &'static str,
    call: fn(&ClientEvent, &Context<D>) -> HandlerFuture<'static>,
}

impl<D> std::fmt::Debug for Handler<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handler").field("name", &self.name).finish()
    }
}

impl<D> Handler<D> {
    /// Builds a handler. Called by the `#[event]` macro; rarely written by hand.
    #[must_use]
    pub fn new(
        name: &'static str,
        call: fn(&ClientEvent, &Context<D>) -> HandlerFuture<'static>,
    ) -> Self {
        Self { name, call }
    }

    /// The handler's name, for logs.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Runs the handler against one event.
    pub fn call(&self, event: &ClientEvent, context: &Context<D>) -> HandlerFuture<'static> {
        (self.call)(event, context)
    }
}

/// Dispatches events to handlers.
///
/// Deliberately not a runtime: it holds handlers and a context, and the application drives
/// it. That keeps ownership of the event loop with the caller, so this composes with a
/// broker, a `select!`, or anything else.
#[derive(Debug)]
pub struct Framework<D> {
    handlers: Vec<Handler<D>>,
    context: Context<D>,
}

impl<D: Send + Sync + 'static> Framework<D> {
    /// Builds a framework over a bot and its state.
    #[must_use]
    pub fn new(bot: crate::Bot, data: D) -> Self {
        Self { handlers: Vec::new(), context: Context::new(bot, Arc::new(data)) }
    }

    /// Registers a handler.
    #[must_use]
    pub fn handler(mut self, handler: Handler<D>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Registers several handlers.
    #[must_use]
    pub fn handlers(mut self, handlers: impl IntoIterator<Item = Handler<D>>) -> Self {
        self.handlers.extend(handlers);
        self
    }

    /// How many handlers are registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether no handler is registered.
    ///
    /// Worth checking at startup: a bot with no handlers connects, logs in and does
    /// nothing, which looks identical to a bot whose server is quiet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// The context handlers receive.
    #[must_use]
    pub fn context(&self) -> &Context<D> {
        &self.context
    }

    /// Offers one event to every handler, sequentially.
    ///
    /// Sequential on purpose: a handler that panics or errors must not take the others
    /// with it, and spawning per handler per event — serenity's model — costs a `Context`
    /// clone and a task for every listener whether or not it wants the event. A caller who
    /// wants concurrency can spawn around this.
    pub async fn dispatch(&self, event: &ClientEvent) {
        for handler in &self.handlers {
            if let Err(error) = handler.call(event, &self.context).await {
                tracing::warn!(handler = handler.name(), %error, "handler failed");
            }
        }
    }
}

/// Extractor for a decoded stream event.
///
/// Handlers that want to match every stream event themselves take this; handlers that want
/// one specific event take that event's own type instead.
#[derive(Debug, Clone)]
pub struct Stream(pub StreamEvent);

impl<D> FromEvent<D> for Stream {
    fn from_event(event: &ClientEvent, _context: &Context<D>) -> Result<Self, Extract> {
        match event {
            ClientEvent::Stream { event, .. } => Ok(Self(event.clone())),
            _ => Err(Extract::Skip),
        }
    }
}

/// Produces a handler's whole parameter list.
///
/// Implemented for tuples so the macro can extract every parameter in one call. A `Skip`
/// from any parameter skips the handler; the common case is the event parameter declining
/// an event it does not handle.
pub trait HandlerArgs<D>: Sized {
    /// Extracts every parameter.
    ///
    /// # Errors
    /// Propagates the first [`Extract`] failure.
    fn extract(event: &ClientEvent, context: &Context<D>) -> Result<Self, Extract>;
}

macro_rules! impl_handler_args {
    ($($name:ident),*) => {
        impl<D, $($name: FromEvent<D>),*> HandlerArgs<D> for ($($name,)*) {
            fn extract(
                _event: &ClientEvent,
                _context: &Context<D>,
            ) -> Result<Self, Extract> {
                Ok(($($name::from_event(_event, _context)?,)*))
            }
        }
    };
}

impl_handler_args!();
impl_handler_args!(A);
impl_handler_args!(A, B);
impl_handler_args!(A, B, C);
impl_handler_args!(A, B, C, D2);
impl_handler_args!(A, B, C, D2, E);
impl_handler_args!(A, B, C, D2, E, F);
impl_handler_args!(A, B, C, D2, E, F, G);
impl_handler_args!(A, B, C, D2, E, F, G, H);

// ---------------------------------------------------------------------------------------
// Event extractors
//
// One type per event a handler can declare. This is what makes the event identity a *type*
// rather than a function name: `#[event] async fn f(m: MessageCreate, ..)` cannot be
// misspelled into silence the way discord.py's `on_message` can.
// ---------------------------------------------------------------------------------------

use rocketsocket_model::entity::Message;
use rocketsocket_model::id::{MessageId, RoomId};

/// A message the bot can see.
///
/// Extracted from both `stream-room-messages` keys: the per-room subscription and the
/// `__my_messages__` catch-all. [`participant`](Self::participant) is `Some` only for the
/// latter, which is the only source that reports it.
#[derive(Debug, Clone)]
pub struct MessageCreate {
    /// The message.
    pub message: Message,
    /// Whether the bot is a participant in the room, when the event reports it.
    ///
    /// `__my_messages__` delivers every room the bot may *read*, not only rooms it has
    /// joined — so a bot that should only act where it is a member must check this.
    pub participant: Option<bool>,
}

impl<D> FromEvent<D> for MessageCreate {
    fn from_event(event: &ClientEvent, _context: &Context<D>) -> Result<Self, Extract> {
        let ClientEvent::Stream { event, .. } = event else {
            return Err(Extract::Skip);
        };
        match event {
            StreamEvent::MyMessage { message, meta } => {
                Ok(Self { message: (**message).clone(), participant: meta.room_participant })
            }
            StreamEvent::RoomMessage { message, .. } => {
                Ok(Self { message: (**message).clone(), participant: None })
            }
            _ => Err(Extract::Skip),
        }
    }
}

/// A message was deleted.
///
/// Rocket.Chat has no global delete stream: this comes from `stream-notify-room`
/// `<rid>/deleteMessage`, so it only arrives for rooms the bot subscribed to individually.
#[derive(Debug, Clone)]
pub struct MessageDeleted {
    /// The room the message was in.
    pub room: RoomId,
    /// The deleted message.
    pub message: MessageId,
}

impl<D> FromEvent<D> for MessageDeleted {
    fn from_event(event: &ClientEvent, _context: &Context<D>) -> Result<Self, Extract> {
        let ClientEvent::Stream { event, .. } = event else {
            return Err(Extract::Skip);
        };
        match event {
            StreamEvent::MessageDeleted { room, message } => {
                Ok(Self { room: room.clone(), message: message.clone() })
            }
            _ => Err(Extract::Skip),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketsocket_realtime::subscription::StreamKey;
    use serde_json::json;

    fn message_event(text: &str) -> ClientEvent {
        let args = vec![
            json!({
                "_id": "m1", "rid": "GENERAL", "msg": text,
                "ts": {"$date": 1_755_529_012_345_i64},
                "u": {"_id": "u1", "username": "alice"},
                "_updatedAt": {"$date": 1_755_529_012_345_i64}
            }),
            json!({"roomParticipant": true, "roomType": "c", "roomName": "general"}),
        ];
        ClientEvent::Stream {
            key: StreamKey::new("room-messages", "__my_messages__"),
            event: StreamEvent::decode("room-messages", "__my_messages__", &args),
            args,
        }
    }

    fn other_event() -> ClientEvent {
        ClientEvent::Resubscribed {
            epoch: rocketsocket_realtime::correlate::Epoch::default(),
            restored: 0,
        }
    }

    #[tokio::test]
    async fn a_message_extractor_takes_the_event_it_declared() {
        let context: Context<()> = Context::new(fake_bot(), Arc::new(()));
        let extracted = MessageCreate::from_event(&message_event("hello"), &context)
            .expect("a message event must extract");

        assert_eq!(extracted.message.msg, "hello");
        assert_eq!(
            extracted.participant,
            Some(true),
            "__my_messages__ reports participation; a per-room subscription does not"
        );
    }

    #[tokio::test]
    async fn an_extractor_skips_an_event_it_does_not_handle() {
        let context: Context<()> = Context::new(fake_bot(), Arc::new(()));

        // Skip, not Failed: declining someone else's event is the ordinary case and must
        // never be reported as a handler failure.
        assert_eq!(MessageCreate::from_event(&other_event(), &context).err(), Some(Extract::Skip));
        assert_eq!(
            MessageDeleted::from_event(&message_event("hi"), &context).err(),
            Some(Extract::Skip)
        );
    }

    #[tokio::test]
    async fn state_and_context_extract_from_any_event() {
        let context: Context<u32> = Context::new(fake_bot(), Arc::new(7));

        let State(data) = State::<u32>::from_event(&other_event(), &context).expect("state");
        assert_eq!(*data, 7);

        let cloned = Context::<u32>::from_event(&other_event(), &context).expect("context");
        assert_eq!(**cloned.data(), 7);
    }

    #[tokio::test]
    async fn a_tuple_extracts_every_parameter_and_propagates_a_skip() {
        let context: Context<u32> = Context::new(fake_bot(), Arc::new(1));

        let ok = <(MessageCreate, State<u32>) as HandlerArgs<u32>>::extract(
            &message_event("hi"),
            &context,
        );
        assert!(ok.is_ok());

        // One skipping parameter skips the whole handler.
        let skipped = <(MessageDeleted, State<u32>) as HandlerArgs<u32>>::extract(
            &message_event("hi"),
            &context,
        );
        assert_eq!(skipped.err(), Some(Extract::Skip));
    }

    #[tokio::test]
    async fn an_empty_framework_is_reported_as_such() {
        // A bot with no handlers looks exactly like a bot on a quiet server.
        let framework = Framework::new(fake_bot(), ());
        assert!(framework.is_empty());
        assert_eq!(framework.len(), 0);
    }

    /// A `Bot` that is never used for IO — the extractors under test never touch it.
    fn fake_bot() -> crate::Bot {
        crate::Bot::for_tests()
    }
}
