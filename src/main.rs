//! A chat server that broadcasts a message to all connections.
//!
//! This example is explicitly more verbose than it has to be. This is to
//! illustrate more concepts.
//!
//! A chat server for telnet clients. After a telnet client connects, the first
//! line should contain the client's name. After that, all lines sent by a
//! client are broadcasted to all other connected clients.
//!
//! Because the client is telnet, lines are delimited by "\r\n".
//!
//! You can test this out by running:
//!
//!     cargo run --example chat
//!
//! And then in another terminal run:
//!
//!     telnet localhost 6142
//!
//! You can run the `telnet` command in any number of additional windows.
//!
//! You can run the second command in multiple windows and then chat between the
//! two, seeing the messages from the other client as they're received. For all
//! connected clients they'll all join the same room and see everyone else's
//! messages.

//#![deny(warnings)]

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::{self, Either};
use futures::sync::mpsc;
use futures::try_ready;
use lazy_static::lazy_static;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use chrono::{DateTime, Local, Timelike};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use AsyncWrite;

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<Bytes>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<Bytes>;

/// Data that is shared between all peers in the chat server.
///
/// This is the set of `Tx` handles for all connected clients. Whenever a
/// message is received from a client, it is broadcasted to all peers by
/// iterating over the `peers` entries and sending a copy of the message on each
/// `Tx`.

enum Service {
    RoomPeers,
    Rooms,
    Room,
}

lazy_static! {
    static ref SERVICE_COMMANDS: HashMap<BytesMut, Service> = {
        let mut commands = HashMap::new();
        commands.insert(BytesMut::from("/peers"), Service::RoomPeers);
        commands.insert(BytesMut::from("/rooms"), Service::Rooms);
        commands.insert(BytesMut::from("/room"), Service::Room);
        commands
    };
}

struct Shared {
    peers: HashMap<SocketAddr, (Tx, BytesMut)>,
}

/// The state for each connected client.
struct Peer {
    /// Name of the peer.
    ///
    /// When a client connects, the first line sent is treated as the client's
    /// name (like alice or bob). The name is used to preface all messages that
    /// arrive from the client so that we can simulate a real chat server:
    ///
    /// ```text
    /// alice: Hello everyone.
    /// bob: Welcome to telnet chat!
    /// ```
    name: BytesMut,

    /// The TCP socket wrapped with the `Lines` codec, defined below.
    ///
    /// This handles sending and receiving data on the socket. When using
    /// `Lines`, we can work at the line level instead of having to manage the
    /// raw byte operations.
    lines: Lines,

    /// Handle to the shared chat state.
    ///
    /// This is used to broadcast messages read off the socket to all connected
    /// peers.
    state: Arc<Mutex<Shared>>,

    /// Receive half of the message channel.
    ///
    /// This is used to receive messages from peers. When a message is received
    /// off of this `Rx`, it will be written to the socket.
    rx: Rx,

    /// Client socket address.
    ///
    /// The socket address is used as the key in the `peers` HashMap. The
    /// address is saved so that the `Peer` drop implementation can clean up its
    /// entry.
    addr: SocketAddr,

    room: BytesMut,

    rooms: Arc<Mutex<SharedRooms>>,
}

/// Line based codec
///
/// This decorates a socket and presents a line based read / write interface.
///
/// As a user of `Lines`, we can focus on working at the line level. So, we send
/// and receive values that represent entire lines. The `Lines` codec will
/// handle the encoding and decoding as well as reading from and writing to the
/// socket.
#[derive(Debug)]
struct Lines {
    /// The TCP socket.
    socket: TcpStream,

    /// Buffer used when reading from the socket. Data is not returned from this
    /// buffer until an entire line has been read.
    rd: BytesMut,

    /// Buffer used to stage data before writing it to the socket.
    wr: BytesMut,
}

struct SharedRooms(HashSet<BytesMut>);

impl SharedRooms {
    fn new() -> Self {
        SharedRooms(HashSet::new())
    }

    fn get_room_names(&self) -> BytesMut {
        let mut i = 0_u32;
        let mut s = BytesMut::from("\r\nRooms list: ");;
        self.0.iter().for_each(|elem| {
            s.extend_from_slice(elem);
            if i % 5 == 0 {
                s.extend_from_slice(b"\r\n");
            } else {
                s.extend_from_slice(b" ");
            }
            i += 1;
        });
        s
    }
}

impl Shared {
    /// Create a new, empty, instance of `Shared`.
    fn new() -> Self {
        Shared {
            peers: HashMap::new(),
        }
    }
}

impl Peer {
    /// Create a new instance of `Peer`.
    fn new(
        name: BytesMut,
        state: Arc<Mutex<Shared>>,
        lines: Lines,
        room: BytesMut,
        rooms: Arc<Mutex<SharedRooms>>,
    ) -> Peer {
        // Get the client socket address
        let addr = lines.socket.peer_addr().unwrap();

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        state.lock().unwrap().peers.insert(addr, (tx, room.clone()));
        rooms.lock().unwrap().0.insert(room.clone());
        Peer {
            name,
            lines,
            state,
            rx,
            addr,
            room,
            rooms,
        }
    }

    fn get_room_peer_names(&self) -> BytesMut {
        let mut i = 0_u32;
        let mut s = BytesMut::from("Available peers: ");
        for (_, (_, room)) in &self.state.lock().unwrap().peers {
            if self.room == room {
                s.extend_from_slice(&self.name);
                if i % 5 == 0 {
                    s.extend_from_slice(b"\r\n");
                } else {
                    s.extend_from_slice(b" ");
                }
                i += 1;
            }
        }
        s
    }

    fn select_room(&mut self) -> Poll<(), io::Error> {
        loop {
            if let Async::Ready(line) = self.lines.poll()? {
                if let Some(room) = line {
                    self.room = room.clone();
                    self.rooms.lock().unwrap().0.insert(room);
                    let (tx, _) = self.state.lock().unwrap().peers.remove(&self.addr).unwrap();
                    self.state.lock().unwrap().peers.insert(self.addr, (tx, self.room.clone()));
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}

/// This is where a connected client is managed.
///
/// A `Peer` is also a future representing completely processing the client.
///
/// When a `Peer` is created, the first line (representing the client's name)
/// has already been read. When the socket closes, the `Peer` future completes.
///
/// While processing, the peer future implementation will:
///
/// 1) Receive messages on its message channel and write them to the socket.
/// 2) Receive messages from the socket and broadcast them to all peers.
///
impl Future for Peer {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        // Tokio (and futures) use cooperative scheduling without any
        // preemption. If a task never yields execution back to the executor,
        // then other tasks may be starved.
        //
        // To deal with this, robust applications should not have any unbounded
        // loops. In this example, we will read at most `LINES_PER_TICK` lines
        // from the client on each tick.
        //
        // If the limit is hit, the current task is notified, informing the
        // executor to schedule the task again asap.
        const LINES_PER_TICK: usize = 10;

        // Receive all messages from peers.
        for i in 0..LINES_PER_TICK {
            // Polling an `UnboundedReceiver` cannot fail, so `unwrap` here is
            // safe.
            match self.rx.poll().unwrap() {
                Async::Ready(Some(v)) => {
                    // Buffer the line. Once all lines are buffered, they will
                    // be flushed to the socket (right below).
                    self.lines.buffer(&v);

                    // If this is the last iteration, the loop will break even
                    // though there could still be lines to read. Because we did
                    // not reach `Async::NotReady`, we have to notify ourselves
                    // in order to tell the executor to schedule the task again.
                    if i + 1 == LINES_PER_TICK {
                        task::current().notify();
                    }
                }
                _ => break,
            }
        }

        // Flush the write buffer to the socket
        let _ = self.lines.poll_flush()?;

        // Read new lines from the socket
        while let Async::Ready(line) = self.lines.poll()? {
            println!(
                "Received line ({:?}) : {:?}) : {:?}",
                self.room, self.name, line
            );
            if let Some(message) = line {
                match SERVICE_COMMANDS.get(&message) {
                    Some(Service::RoomPeers) => {
                        let _ = self.lines.socket.write_all(&self.get_room_peer_names());
                        continue;
                    }
                    Some(Service::Rooms) => {
                        let _ = self.lines.socket.write_all(b"\r\nSelect room to shift: ");
                        let _ = self
                            .lines
                            .socket
                            .write_all(&self.rooms.lock().unwrap().get_room_names());
                        let _ = self.select_room();
                        continue;
                    }
                    Some(Service::Room) => {
                        let _ = self.lines.socket.write_all(b"\r\nCurrent room: ");
                        let _ = self.lines.socket.write_all(&self.room);
                        continue;
                    }
                    None => {
                        // Append the peer's name to the front of the line:
                        let dt: DateTime<Local> = Local::now();
                        let date = format!("{}:{}:{} ", dt.hour(), dt.minute(), dt.second());
                        let mut line = BytesMut::from(date);
                        line.extend(&self.name);
                        line.extend_from_slice(b": ");
                        line.extend_from_slice(&message);
                        line.extend_from_slice(b"\r\n");

                        // We're using `Bytes`, which allows zero-copy clones (by
                        // storing the data in an Arc internally).
                        //
                        // However, before cloning, we must freeze the data. This
                        // converts it from mutable -> immutable, allowing zero copy
                        // cloning.
                        let line = line.freeze();

                        // Now, send the line to all other peers
                        for (addr, (tx, room)) in &self.state.lock().unwrap().peers {
                            // Don't send the message to ourselves
                            if *addr != self.addr && *room == self.room {
                                // The send only fails if the rx half has been dropped,
                                // however this is impossible as the `tx` half will be
                                // removed from the map before the `rx` is dropped.
                                tx.unbounded_send(line.clone()).unwrap();
                            }
                        }
                    }
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                return Ok(Async::Ready(()));
            }
        }

        // As always, it is important to not just return `NotReady` without
        // ensuring an inner future also returned `NotReady`.
        //
        // We know we got a `NotReady` from either `self.rx` or `self.lines`, so
        // the contract is respected.
        Ok(Async::NotReady)
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let peers = &mut self.state.lock().unwrap().peers;
        peers.remove(&self.addr);
        if peers.iter().all(|(_, (_, room))| *room != self.room) {
            self.rooms.lock().unwrap().0.remove(&self.room);
        }
    }
}

impl Lines {
    /// Create a new `Lines` codec backed by the socket
    fn new(socket: TcpStream) -> Self {
        Lines {
            socket,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    /// Buffer a line.
    ///
    /// This writes the line to an internal buffer. Calls to `poll_flush` will
    /// attempt to flush this buffer to the socket.
    fn buffer(&mut self, line: &[u8]) {
        // Ensure the buffer has capacity. Ideally this would not be unbounded,
        // but to keep the example simple, we will not limit this.
        self.wr.reserve(line.len());

        // Push the line onto the end of the write buffer.
        //
        // The `put` function is from the `BufMut` trait.
        self.wr.put(line);
    }

    /// Flush the write buffer to the socket
    fn poll_flush(&mut self) -> Poll<(), io::Error> {
        // As long as there is buffered data to write, try to write it.
        while !self.wr.is_empty() {
            // Try to write some bytes to the socket
            let n = try_ready!(self.socket.poll_write(&self.wr));

            // As long as the wr is not empty, a successful write should
            // never write 0 bytes.
            assert!(n > 0);

            // This discards the first `n` bytes of the buffer.
            let _ = self.wr.split_to(n);
        }

        Ok(Async::Ready(()))
    }

    /// Read data from the socket.
    ///
    /// This only returns `Ready` when the socket has closed.
    fn fill_read_buf(&mut self) -> Poll<(), io::Error> {
        loop {
            // Ensure the read buffer has capacity.
            //
            // This might result in an internal allocation.
            self.rd.reserve(1024);

            // Read data into the buffer.
            let n = try_ready!(self.socket.read_buf(&mut self.rd));

            if n == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl Stream for Lines {
    type Item = BytesMut;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // First, read any new data that might have been received off the socket
        let sock_closed = self.fill_read_buf()?.is_ready();
        // Now, try finding lines
        let pos = self
            .rd
            .windows(2)
            .enumerate()
            .find(|&(_, bytes)| bytes == b"\r\n")
            .map(|(i, _)| i);

        if let Some(pos) = pos {
            println!("Line received!");
            // Remove the line from the read buffer and set it to `line`.
            let mut line = self.rd.split_to(pos + 2);

            // Drop the trailing \r\n
            line.split_off(pos);

            // Return the line
            return Ok(Async::Ready(Some(line)));
        }

        if sock_closed {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// Spawn a task to manage the socket.
///
/// This will read the first line from the socket to identify the client, then
/// add the client to the set of connected peers in the chat service.
fn process(socket: TcpStream, rooms: Arc<Mutex<SharedRooms>>, state: Arc<Mutex<Shared>>) {
    // Wrap the socket with the `Lines` codec that we wrote above.
    //
    // By doing this, we can operate at the line level instead of doing raw byte
    // manipulation.
    //"Please, select room or create a new one: ");

    let lines = Lines::new(socket);

    // The first line is treated as the client's name. The client is not added
    // to the set of connected peers until this line is received.
    //
    // We use the `into_future` combinator to extract the first item from the
    // lines stream. `into_future` takes a `Stream` and converts it to a future
    // of `(first, rest)` where `rest` is the original stream instance.
    let connection = lines
        .into_future()
        // `into_future` doesn't have the right error type, so map the error to
        // make it work.
        .map_err(|(e, _)| e)
        // Process the first received line as the client's name.
        .and_then(|(room, mut lines)| {
            let _ = lines.socket.write_all(b"\r\nSelect a nickname:\r\n");

            // If `name` is `None`, then the client disconnected without
            // actually sending a line of data.
            //
            // Since the connection is closed, there is no further work that we
            // need to do. So, we just terminate processing by returning
            // `future::ok()`.
            //
            // The problem is that only a single future type can be returned
            // from a combinator closure, but we want to return both
            // `future::ok()` and `Peer` (below).
            //
            // This is a common problem, so the `futures` crate solves this by
            // providing the `Either` helper enum that allows creating a single
            // return type that covers two concrete future types.
            let room = match room {
                Some(room) => room,
                None => {
                    // The remote client closed the connection without sending
                    // any data.
                    return Either::A(future::ok(()));
                }
            };

            println!("joining the room: `{:?}`", room);
            let connect = lines
                .into_future()
                .map_err(|(e, _)| e)
                .and_then(|(name, lines)| {
                    let name = match name {
                        Some(name) => name,
                        None => {
                            // The remote client closed the connection without sending
                            // any data.
                            return Either::A(future::ok(()));
                        }
                    };
                    let peer = Peer::new(name, state, lines, room, rooms);
                    // Wrap `peer` with `Either::B` to make the return type fit.
                    Either::B(peer)
                });
            Either::B(connect)
        })
        .map_err(|e| {
            println!("connection error = {:?}", e);
        });

    // Task futures have an error of type `()`, this ensures we handle the
    // error. We do this by printing the error to STDOUT.
    // Spawn the task. Internally, this submits the task to a thread pool.
    tokio::spawn(connection);
}

pub fn main() -> Result<(), Box<std::error::Error>> {
    // Create the shared state. This is how all the peers communicate.
    //
    // The server task will hold a handle to this. For every new client, the
    // `state` handle is cloned and passed into the task that processes the
    // client connection.
    let state = Arc::new(Mutex::new(Shared::new()));

    let rooms = Arc::new(Mutex::new(SharedRooms::new()));

    let addr = "127.0.0.1:6142".parse()?;

    // Bind a TCP listener to the socket address.
    //
    // Note that this is the Tokio TcpListener, which is fully async.
    let listener = TcpListener::bind(&addr)?;

    // The server task asynchronously iterates over and processes each
    // incoming connection.
    let server = listener
        .incoming()
        .for_each(move |socket| {
            let rooms_list = rooms.lock().unwrap().get_room_names();
            let rooms = rooms.clone();
            let state = state.clone();

            let connect = io::write_all(socket, "Please, select a room or create a new one:")
                .and_then(|(socket, _)| io::write_all(socket, rooms_list))
                .and_then(|(socket, _)| {
                    process(socket, rooms, state);
                    Ok(())
                })
                .map_err(|e| {
                    println!("connection error = {:?}", e);
                });
            tokio::spawn(connect);
            Ok(())
        })
        .map_err(|err| {
            // All tasks must have an `Error` type of `()`. This forces error
            // handling and helps avoid silencing failures.
            //
            // In our example, we are only going to log the error to STDOUT.
            println!("accept error = {:?}", err);
        });

    println!("server running on localhost:6142");

    // Start the Tokio runtime.
    //
    // The Tokio is a pre-configured "out of the box" runtime for building
    // asynchronous applications. It includes both a reactor and a task
    // scheduler. This means applications are multithreaded by default.
    //
    // This function blocks until the runtime reaches an idle state. Idle is
    // defined as all spawned tasks have completed and all I/O resources (TCP
    // sockets in our case) have been dropped.
    //
    // In our example, we have not defined a shutdown strategy, so this will
    // block until `ctrl-c` is pressed at the terminal.
    tokio::run(server);
    Ok(())
}
