/*
 * NU-Info Exchange System
 * Campus Client (Lahore / Karachi / Peshawar / CFD / Multan)
 *
 * Usage  :  ./client <CampusName>
 * Example:  ./client Lahore
 *
 * Build  :  g++ -std=c++17 client.cpp -o client -lpthread
 */

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define SERVER_IP    "127.0.0.1"
#define TCP_PORT     9000
#define UDP_SRV_PORT 9001   // server's UDP port (heartbeats go here)
#define BUF_SIZE     4096
#define HB_INTERVAL  10

using namespace std;

// Login credentials (must match server side)
const unordered_map<string, string> CREDENTIALS = {
    {"Lahore",   "NU-LHR-123"},
    {"Karachi",  "NU-KHI-123"},
    {"Peshawar", "NU-PSH-123"},
    {"CFD",      "NU-CFD-123"},
    {"Multan",   "NU-MLT-123"}
};

// Each campus gets its own fixed receive port so multiple clients
// can coexist on the same machine without a bind() collision.
const unordered_map<string, int> CAMPUS_UDP_RECV_PORT = {
    {"Lahore",   9101},
    {"Karachi",  9102},
    {"Peshawar", 9103},
    {"CFD",      9104},
    {"Multan",   9105}
};

// Departments available for selection
const vector<string> DEPARTMENTS = {
    "Admissions", "Academics", "IT", "Sports"
};

// 
string g_campusName;
string g_currentDept = "Admissions";
int    g_recvPort    = 9101;          // filled from CAMPUS_UDP_RECV_PORT at startup

atomic<bool> g_running(true);

vector<string> g_inbox;
mutex          g_inboxMutex;


// 

string currentTime()
{
    time_t now = time(nullptr);
    char buf[64];
    strftime(buf, sizeof(buf), "%H:%M:%S", localtime(&now));
    return string(buf);
}

vector<string> split(const string &s, char delim)
{
    vector<string> tokens;
    istringstream ss(s);
    string tok;
    while (getline(ss, tok, delim))
        tokens.push_back(tok);
    return tokens;
}

bool sendTCP(int sock, const string &msg)
{
    string framed = msg + "\n";
    ssize_t sent = send(sock, framed.c_str(), framed.size(), 0);
    return sent == (ssize_t)framed.size();
}


// 
// Listens for incoming TCP messages from server.
void tcpReceiveThread(int tcpSock)
{
    char buf[BUF_SIZE];
    string recvBuf;

    while (g_running)
    {
        memset(buf, 0, BUF_SIZE);
        ssize_t n = recv(tcpSock, buf, BUF_SIZE - 1, 0);

        if (n <= 0) {
            if (g_running) {
                cout << "\n[CLIENT] Server disconnected." << endl;
                g_running = false;
            }
            break;
        }

        recvBuf += string(buf, n);

        size_t pos;
        while ((pos = recvBuf.find('\n')) != string::npos)
        {
            string msg = recvBuf.substr(0, pos);
            recvBuf.erase(0, pos + 1);
            if (msg.empty()) continue;

            auto fields = split(msg, '|');

            if (fields[0] == "MSG" && fields.size() >= 6)
            {
                string entry = "[" + currentTime() + "] FROM " +
                               fields[1] + "/" + fields[2] + ": " + fields[5];
                {
                    lock_guard<mutex> lock(g_inboxMutex);
                    g_inbox.push_back(entry);
                }
                cout << "\n\n  ┌─ New Message ──────────────────────────────┐" << endl;
                cout << "  │ From : " << fields[1] << " — " << fields[2] << endl;
                cout << "  │ To   : " << fields[3] << " — " << fields[4] << endl;
                cout << "  │ Msg  : " << fields[5] << endl;
                cout << "  └────────────────────────────────────────────┘" << endl;
                cout << "Enter choice: " << flush;
            }
            else if (fields[0] == "DELIVERED")
            {
                cout << "\n  [✓] Message delivered to "
                     << (fields.size() > 1 ? fields[1] : "?") << endl;
            }
            else if (fields[0] == "AUTH")
            {
                if (fields.size() > 1 && fields[1] == "OK")
                    cout << "[AUTH]  " << (fields.size() > 2 ? fields[2] : "OK") << endl;
                else
                    cout << "[AUTH]  FAILED: "
                         << (fields.size() > 2 ? fields[2] : "unknown") << endl;
            }
            else if (fields[0] == "ERROR")
            {
                cout << "\n  [ERROR] "
                     << (fields.size() > 1 ? fields[1] : "Unknown error") << endl;
            }
        }
    }
}



// Uses udpSendSock (unbound, ephemeral port) to send heartbeats.
// The HEARTBEAT packet now includes the campus's dedicated receive-port
// so the server knows where to send broadcasts back.
//
// Packet format:  HEARTBEAT|<CampusName>|<Timestamp>|<RecvPort>
//
void heartbeatThread(int udpSendSock, sockaddr_in serverUdpAddr)
{
    while (g_running)
    {
        // Include g_recvPort so the server can address broadcasts correctly
        string pkt = "HEARTBEAT|" + g_campusName + "|" +
                     currentTime() + "|" + to_string(g_recvPort);

        sendto(udpSendSock, pkt.c_str(), pkt.size(), 0,
               (sockaddr*)&serverUdpAddr, sizeof(serverUdpAddr));

        for (int i = 0; i < HB_INTERVAL && g_running; ++i)
            this_thread::sleep_for(chrono::seconds(1));
    }
}


// Uses udpRecvSock which is bound to the campus-specific port (9101-9105).
// This socket never sends anything — receive only.
void udpBroadcastListenThread(int udpRecvSock)
{
    char buf[BUF_SIZE];

    while (g_running)
    {
        sockaddr_in sender{};
        socklen_t   senderLen = sizeof(sender);

        memset(buf, 0, BUF_SIZE);
        ssize_t n = recvfrom(udpRecvSock, buf, BUF_SIZE - 1, 0,
                             (sockaddr*)&sender, &senderLen);
        if (n <= 0) continue;

        string pkt(buf, n);
        auto fields = split(pkt, '|');

        if (fields[0] == "BROADCAST" && fields.size() >= 2)
        {
            cout << "\n\n  ╔══ ADMIN BROADCAST ════════════════════════╗" << endl;
            cout << "  ║  " << fields[1] << endl;
            cout << "  ╚═══════════════════════════════════════════╝" << endl;
            cout << "Enter choice: " << flush;
        }
    }
}



void showInbox()
{
    lock_guard<mutex> lock(g_inboxMutex);
    if (g_inbox.empty()) {
        cout << "  (No messages received yet.)" << endl;
    } else {
        cout << "\n  ── Inbox (" << g_inbox.size() << " message(s)) ──" << endl;
        for (size_t i = 0; i < g_inbox.size(); ++i)
            cout << "  [" << (i+1) << "] " << g_inbox[i] << endl;
    }
}

void sendMessage(int tcpSock)
{
    string targetCampus, targetDept, msgText;

    cout << "\n  Enter target campus  : ";
    getline(cin, targetCampus);
    cout << "  Enter target dept    : ";
    getline(cin, targetDept);
    cout << "  Enter your message   : ";
    getline(cin, msgText);

    if (targetCampus.empty() || targetDept.empty() || msgText.empty()) {
        cout << "  [ERROR] All fields required." << endl;
        return;
    }

    string pkt = "MSG|" + g_campusName + "|" + g_currentDept +
                 "|" + targetCampus + "|" + targetDept + "|" + msgText;

    if (sendTCP(tcpSock, pkt))
        cout << "\n  [SENT] " << pkt << endl;
    else
        cout << "  [ERROR] Failed to send message." << endl;
}

void selectDepartment()
{
    cout << "\n  Select your department:" << endl;
    for (size_t i = 0; i < DEPARTMENTS.size(); ++i)
        cout << "    " << (i+1) << ". " << DEPARTMENTS[i] << endl;
    cout << "  Choice: ";

    string input;
    getline(cin, input);
    int choice = 0;
    try { choice = stoi(input); } catch (...) {}

    if (choice >= 1 && choice <= (int)DEPARTMENTS.size()) {
        g_currentDept = DEPARTMENTS[choice - 1];
        cout << "  Department set to: " << g_currentDept << endl;
    } else {
        cout << "  Invalid choice." << endl;
    }
}

void mainMenu(int tcpSock)
{
    while (g_running)
    {
        cout << "\n╔══════════════════════════════════════════╗" << endl;
        cout << "║  NU Campus Client — " << g_campusName;
        int pad = 22 - (int)g_campusName.size();
        for (int i = 0; i < pad; ++i) cout << ' ';
        cout << "║" << endl;

        cout << "║  Active Dept: " << g_currentDept;
        pad = 26 - (int)g_currentDept.size();
        for (int i = 0; i < pad; ++i) cout << ' ';
        cout << "║" << endl;

        cout << "╠══════════════════════════════════════════╣" << endl;
        cout << "║  1. Send a message to another campus     ║" << endl;
        cout << "║  2. View received messages (inbox)       ║" << endl;
        cout << "║  3. Change active department             ║" << endl;
        cout << "║  4. Exit                                 ║" << endl;
        cout << "╚══════════════════════════════════════════╝" << endl;
        cout << "Enter choice: ";

        string input;
        if (!getline(cin, input)) { g_running = false; break; }

        int choice = 0;
        try { choice = stoi(input); } catch (...) {}

        switch (choice)
        {
            case 1: sendMessage(tcpSock);  break;
            case 2: showInbox();           break;
            case 3: selectDepartment();    break;
            case 4:
                g_running = false;
                cout << "  Goodbye!" << endl;
                break;
            default:
                cout << "  Invalid option. Try again." << endl;
        }
    }
}


int main(int argc, char *argv[])
{
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <CampusName>" << endl;
        cerr << "Valid campuses: Lahore, Karachi, Peshawar, CFD, Multan" << endl;
        return 1;
    }

    g_campusName = argv[1];

    auto credIt = CREDENTIALS.find(g_campusName);
    if (credIt == CREDENTIALS.end()) {
        cerr << "[ERROR] Unknown campus: " << g_campusName << endl;
        return 1;
    }

    auto portIt = CAMPUS_UDP_RECV_PORT.find(g_campusName);
    if (portIt == CAMPUS_UDP_RECV_PORT.end()) {
        cerr << "[ERROR] No receive port defined for campus: " << g_campusName << endl;
        return 1;
    }
    g_recvPort = portIt->second;

    cout << "╔══════════════════════════════════════════╗" << endl;
    cout << "║  NU-Information Exchange System          ║" << endl;
    cout << "║  Campus Client — " << g_campusName;
    int pad = 23 - (int)g_campusName.size();
    for (int i = 0; i < pad; ++i) cout << ' ';
    cout << "║" << endl;
    cout << "║  Broadcast receive port: " << g_recvPort;
    pad = 15 - to_string(g_recvPort).size();
    for (int i = 0; i < pad; ++i) cout << ' ';
    cout << "║" << endl;
    cout << "╚══════════════════════════════════════════╝" << endl;

  
    int tcpSock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcpSock < 0) { perror("TCP socket"); return 1; }

    sockaddr_in serverTcpAddr{};
    serverTcpAddr.sin_family = AF_INET;
    serverTcpAddr.sin_port   = htons(TCP_PORT);
    inet_pton(AF_INET, SERVER_IP, &serverTcpAddr.sin_addr);

    cout << "[CLIENT] Connecting to server..." << endl;
    if (connect(tcpSock, (sockaddr*)&serverTcpAddr, sizeof(serverTcpAddr)) < 0) {
        perror("TCP connect");
        return 1;
    }
    cout << "[CLIENT] Connected." << endl;

    // Send authentication immediately after connecting
    string authMsg = "AUTH|Campus:" + g_campusName + ",Pass:" + credIt->second;
    sendTCP(tcpSock, authMsg);

    // ── UDP send socket (heartbeats, ephemeral port)
    int udpSendSock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSendSock < 0) { perror("UDP send socket"); return 1; }
    // Intentionally NOT bound — the OS assigns an ephemeral source port.

    sockaddr_in serverUdpAddr{};
    serverUdpAddr.sin_family = AF_INET;
    serverUdpAddr.sin_port   = htons(UDP_SRV_PORT);
    inet_pton(AF_INET, SERVER_IP, &serverUdpAddr.sin_addr);

    // ── UDP receive socket (broadcasts, campus-specific port) ──
    int udpRecvSock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpRecvSock < 0) { perror("UDP recv socket"); return 1; }

    int opt = 1;
    setsockopt(udpRecvSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in recvBindAddr{};
    recvBindAddr.sin_family      = AF_INET;
    recvBindAddr.sin_addr.s_addr = INADDR_ANY;
    recvBindAddr.sin_port        = htons(g_recvPort);

    if (bind(udpRecvSock, (sockaddr*)&recvBindAddr, sizeof(recvBindAddr)) < 0) {
        perror("UDP recv bind");
        cerr << "[ERROR] Could not bind broadcast receive port " << g_recvPort << endl;
        return 1;
    }
    cout << "[CLIENT] Broadcast listener bound to port " << g_recvPort << endl;


    thread(tcpReceiveThread, tcpSock).detach();
    thread(heartbeatThread, udpSendSock, serverUdpAddr).detach();
    thread(udpBroadcastListenThread, udpRecvSock).detach();

    this_thread::sleep_for(chrono::milliseconds(300));

   
    mainMenu(tcpSock);

    g_running = false;
    close(tcpSock);
    close(udpSendSock);
    close(udpRecvSock);
    cout << "[CLIENT] Disconnected." << endl;
    return 0;
}