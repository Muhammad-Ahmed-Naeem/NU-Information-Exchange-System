 NU-Information Exchange System

A multi-campus messaging system built in C++ using TCP and UDP socket programming. Simulates the network structure of FAST-NUCES by connecting campus nodes through a central server.

---

## Overview

The system follows a hub-and-spoke model. The **Islamabad server** acts as the central router — all campus clients connect to it, authenticate, and route messages through it. No direct campus-to-campus connection exists.

- Campus clients send direct messages to each other over **TCP**
- Clients send periodic **UDP heartbeats** to the server every 10 seconds
- The server admin can push **UDP broadcast announcements** to all connected campuses

---

## Architecture

```
[ Lahore ]  [ Karachi ]  [ Peshawar ]  [ CFD ]  [ Multan ]
     |            |            |           |          |
     |        TCP:9000         |           |          |
     +------------+-----------+-----------+----------+
                              |
                   +----------+----------+
                   |    Central Server   |
                   |  (Islamabad Campus) |
                   |  TCP:9000 UDP:9001  |
                   +----------+----------+
                              |
              +---------------+---------------+
              |               |               |
        UDP:9101         UDP:9102        UDP:9103 ...
        (Lahore)         (Karachi)       (Peshawar)

  Heartbeat → HEARTBEAT|<Campus>|<Time>|<RecvPort>
  Broadcast ← BROADCAST|<Message>
```

Each campus binds a **unique UDP port** (9101–9105) for receiving broadcasts. The server learns this port from the heartbeat packet and uses it when broadcasting — this is what allows multiple clients to run on the same machine without conflicts.

---

## Protocol

All messages are pipe-delimited (`|`) text. TCP messages are newline-terminated (`\n`).

| Type | Transport | Format |
|---|---|---|
| Login | TCP | `AUTH\|Campus:<Name>,Pass:<Password>` |
| Auth response | TCP | `AUTH\|OK\|Welcome <Campus>` or `AUTH\|FAIL\|<reason>` |
| Direct message | TCP | `MSG\|SrcCampus\|SrcDept\|DstCampus\|DstDept\|Text` |
| Delivery confirm | TCP | `DELIVERED\|<DstCampus>` |
| Heartbeat | UDP | `HEARTBEAT\|<Campus>\|<HH:MM:SS>\|<RecvPort>` |
| Broadcast | UDP | `BROADCAST\|<MessageText>` |

---

## Campus Credentials

| Campus | Password |
|---|---|
| Lahore | `NU-LHR-123` |
| Karachi | `NU-KHI-123` |
| Peshawar | `NU-PSH-123` |
| CFD | `NU-CFD-123` |
| Multan | `NU-MLT-123` |

---

## Port Reference

| Port | Protocol | Used for |
|---|---|---|
| 9000 | TCP | Campus client connections |
| 9001 | UDP | Server receives heartbeats |
| 9101 | UDP | Lahore broadcast receive |
| 9102 | UDP | Karachi broadcast receive |
| 9103 | UDP | Peshawar broadcast receive |
| 9104 | UDP | CFD broadcast receive |
| 9105 | UDP | Multan broadcast receive |

---

## Build

**Requirements:** g++ with C++17 support, pthreads (Linux/macOS/WSL)

```bash
g++ -std=c++17 server.cpp -o server -lpthread
g++ -std=c++17 client.cpp -o client -lpthread
```

**Windows (MinGW — replace pthreads with Winsock):**
```bash
g++ -std=c++17 server.cpp -o server.exe -lws2_32
g++ -std=c++17 client.cpp -o client.exe -lws2_32
```

> WSL is recommended on Windows — build commands are identical to Linux.

---

## Run

Start the server first, then open a separate terminal for each campus client.

```bash
# Terminal 1 — start the server
./server

# Terminal 2
./client Lahore

# Terminal 3
./client Karachi

# Terminal 4
./client Peshawar
```

The client connects, authenticates, and drops into a menu:

```
╔══════════════════════════════════════════╗
║  NU Campus Client — Lahore              ║
║  Active Dept: Admissions                ║
╠══════════════════════════════════════════╣
║  1. Send a message to another campus    ║
║  2. View received messages (inbox)      ║
║  3. Change active department            ║
║  4. Exit                                ║
╚══════════════════════════════════════════╝
```

---

## Admin Console

The server terminal doubles as the admin console. Available commands:

```bash
list                        # show all connected campuses + last heartbeat time
broadcast <message>         # send a UDP announcement to all campuses
quit                        # shut down the server
```

Example:
```
broadcast Exams start Monday. All campuses report by 08:00.
```

All connected campus terminals will display:
```
  ╔══ ADMIN BROADCAST ════════════════════════╗
  ║  Exams start Monday. All campuses report by 08:00.
  ╚═══════════════════════════════════════════╝
```

---

## Sending a Message

From any campus client, choose option 1:

```
Enter target campus  : Karachi
Enter target dept    : Academics
Enter your message   : Please send the transfer records by Friday.

[SENT] MSG|Lahore|Admissions|Karachi|Academics|Please send the transfer records by Friday.
```

The server routes it and the Karachi terminal receives:

```
  ┌─ New Message ──────────────────────────────┐
  │ From : Lahore — Admissions
  │ To   : Karachi — Academics
  │ Msg  : Please send the transfer records by Friday.
  └────────────────────────────────────────────┘
```

---

## Departments

Each campus session can switch between four departments:

- Admissions
- Academics
- IT
- Sports

Change the active department via option 3 in the menu.

---

## Concurrency Model

**Server threads:**
| Thread | Role |
|---|---|
| Main / accept loop | Spawns one `handleClient` thread per incoming TCP connection |
| `handleClient` × N | Handles AUTH and MSG for one campus; cleans up on disconnect |
| `udpListenerThread` | Receives heartbeats; updates campus broadcast port |
| `adminThread` | Reads admin console input without blocking the accept loop |

**Client threads:**
| Thread | Role |
|---|---|
| Main / menu loop | Handles user input |
| `tcpReceiveThread` | Receives incoming messages and server responses |
| `heartbeatThread` | Sends UDP heartbeat every 10 seconds |
| `udpBroadcastListenThread` | Listens for admin broadcasts on the campus-specific port |

Shared state on the server (`connectedCampuses` map) is protected with `std::mutex`.

---

## File Structure

```
.
├── server.cpp      # Central server (Islamabad)
├── client.cpp      # Campus client (Lahore / Karachi / Peshawar / CFD / Multan)
└── README.md
```

---

## Notes

- All terminals must be on the same machine for the default config (`SERVER_IP = 127.0.0.1`). For LAN testing, update `SERVER_IP` in `client.cpp` to your server machine's local IP.
- Make sure ports 9000, 9001, and 9101–9105 are not blocked by a firewall.
- Messages are not persisted — if a destination campus is offline when a message is sent, it is lost.
- Credentials are hard-coded in both files and must match.
