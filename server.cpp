/*
 * NU-Information Exchange System
 * Central Server — Islamabad Campus (Hub / Router)
 *
 * Ports  :  TCP 9000        (campus connections)
 *           UDP 9001        (receives heartbeats)
 *           UDP 9101-9105   (sends broadcasts to each campus's receive port)
 *
 * Build  :  g++ -std=c++17 server.cpp -o server -lpthread
 * Run    :  ./server
 */

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <vector>
#include <cstring>
#include <ctime>
#include <atomic>
#include <algorithm>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define TCP_PORT   9000
#define UDP_PORT   9001
#define BUF_SIZE   4096

using namespace std;

// Campus login credentials (must match client side)
const unordered_map<string, string> CREDENTIALS = {
    {"Lahore",   "NU-LHR-123"},
    {"Karachi",  "NU-KHI-123"},
    {"Peshawar", "NU-PSH-123"},
    {"CFD",      "NU-CFD-123"},
    {"Multan",   "NU-MLT-123"}
};

// Stores runtime information for each connected campus
struct CampusInfo {
    int         tcpSocket;
    string      lastHeartbeat;
    string      ipAddress;       // IP from which heartbeats arrive
    int         broadcastPort;   // campus-specific UDP receive port (9101-9105)
    bool        broadcastReady;  // true once we have received at least one heartbeat

    CampusInfo() : tcpSocket(-1), broadcastPort(0), broadcastReady(false) {}
};

// Shared map of connected campuses (protected by campusMutex)
unordered_map<string, CampusInfo> connectedCampuses;
mutex campusMutex;

// Global UDP socket for receiving heartbeats and sending broadcasts
int g_udpSock = -1;

// Flag to control server execution
atomic<bool> g_running(true);




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



// Runs in its own thread for each campus client TCP connection.
void handleClient(int clientSock, sockaddr_in clientAddr)
{
    char   buf[BUF_SIZE];
    string campusName;
    bool   authenticated = false;

    string ipStr = inet_ntoa(clientAddr.sin_addr);
    cout << "[SERVER] New TCP connection from " << ipStr << endl;

    string recvBuf;

    while (g_running)
    {
        memset(buf, 0, BUF_SIZE);
        ssize_t n = recv(clientSock, buf, BUF_SIZE - 1, 0);
        if (n <= 0) break;

        recvBuf += string(buf, n);

        size_t pos;
        while ((pos = recvBuf.find('\n')) != string::npos)
        {
            string msg = recvBuf.substr(0, pos);
            recvBuf.erase(0, pos + 1);
            if (msg.empty()) continue;

            auto fields = split(msg, '|');

            // AUTH
            if (fields[0] == "AUTH" && !authenticated)
            {
                if (fields.size() < 2) {
                    sendTCP(clientSock, "AUTH|FAIL|Bad format");
                    continue;
                }

                string campus, pass;
                auto parts = split(fields[1], ',');
                for (auto &p : parts) {
                    auto kv = split(p, ':');
                    if (kv.size() == 2) {
                        if (kv[0] == "Campus") campus = kv[1];
                        if (kv[0] == "Pass")   pass   = kv[1];
                    }
                }

                auto it = CREDENTIALS.find(campus);
                if (it != CREDENTIALS.end() && it->second == pass)
                {
                    campusName    = campus;
                    authenticated = true;

                    {
                        lock_guard<mutex> lock(campusMutex);
                        connectedCampuses[campusName].tcpSocket  = clientSock;
                        connectedCampuses[campusName].ipAddress  = ipStr;
                        connectedCampuses[campusName].lastHeartbeat = currentTime();
                    }

                    sendTCP(clientSock, "AUTH|OK|Welcome " + campusName);
                    cout << "[AUTH] Campus '" << campusName << "' authenticated." << endl;
                }
                else
                {
                    sendTCP(clientSock, "AUTH|FAIL|Invalid credentials");
                    cout << "[AUTH] Failed login for '" << campus
                         << "' from " << ipStr << endl;
                }
            }

         
            else if (fields[0] == "MSG" && authenticated)
            {
                if (fields.size() < 6) {
                    sendTCP(clientSock, "ERROR|Bad MSG format");
                    continue;
                }

                string dstCampus = fields[3];

                cout << "[ROUTE] " << fields[1] << "/" << fields[2]
                     << " -> " << dstCampus << "/" << fields[4]
                     << " | " << fields[5] << endl;

                lock_guard<mutex> lock(campusMutex);
                auto it = connectedCampuses.find(dstCampus);

                if (it != connectedCampuses.end())
                {
                    sendTCP(it->second.tcpSocket, msg);
                    sendTCP(clientSock, "DELIVERED|" + dstCampus);
                }
                else
                {
                    sendTCP(clientSock, "ERROR|Campus " + dstCampus + " not connected");
                    cout << "[ROUTE] Destination '" << dstCampus << "' not connected." << endl;
                }
            }

            else if (!authenticated)
            {
                sendTCP(clientSock, "ERROR|Please authenticate first");
            }
            else
            {
                cout << "[SERVER] Unknown message from "
                     << campusName << ": " << msg << endl;
            }
        }
    }

    close(clientSock);
    if (!campusName.empty())
    {
        lock_guard<mutex> lock(campusMutex);
        connectedCampuses.erase(campusName);
        cout << "[SERVER] Campus '" << campusName << "' disconnected." << endl;
    }
    else
    {
        cout << "[SERVER] Unauthenticated client " << ipStr << " disconnected." << endl;
    }
}



// Receives HEARTBEAT datagrams and extracts the campus-specific
// broadcast-receive port from the 4th field of the packet.
//
// Heartbeat format: HEARTBEAT|<CampusName>|<Timestamp>|<RecvPort>
//
void udpListenerThread()
{
    char buf[BUF_SIZE];

    while (g_running)
    {
        sockaddr_in senderAddr{};
        socklen_t   addrLen = sizeof(senderAddr);

        memset(buf, 0, BUF_SIZE);
        ssize_t n = recvfrom(g_udpSock, buf, BUF_SIZE - 1, 0,
                             (sockaddr*)&senderAddr, &addrLen);
        if (n <= 0) continue;

        string pkt(buf, n);
        auto fields = split(pkt, '|');

        // Expect at least 4 fields: HEARTBEAT | campus | time | recvPort
        if (fields[0] == "HEARTBEAT" && fields.size() >= 4)
        {
            string campus   = fields[1];
            string ts       = fields[2];
            int    recvPort = 0;

            try { recvPort = stoi(fields[3]); } catch (...) {}

            lock_guard<mutex> lock(campusMutex);
            if (connectedCampuses.count(campus) && recvPort > 0)
            {
                connectedCampuses[campus].lastHeartbeat = ts;
                // Store the campus IP (from senderAddr) and its dedicated receive port
                connectedCampuses[campus].ipAddress     = inet_ntoa(senderAddr.sin_addr);
                connectedCampuses[campus].broadcastPort = recvPort;
                connectedCampuses[campus].broadcastReady = true;
            }

            cout << "[UDP] Heartbeat from " << campus
                 << " at " << ts
                 << " (bcast port " << recvPort << ")" << endl;
        }
    }
}



// Console interface for the server operator.
void adminThread()
{
    cout << "\n[ADMIN] Console ready." << endl;
    cout << "[ADMIN] Commands: list | broadcast <msg> | quit" << endl;

    string line;
    while (g_running && getline(cin, line))
    {
        if (line.empty()) continue;


        if (line == "list")
        {
            lock_guard<mutex> lock(campusMutex);
            cout << "\n=== Connected Campuses ===" << endl;
            if (connectedCampuses.empty()) {
                cout << "  (none)" << endl;
            } else {
                for (auto &[name, info] : connectedCampuses)
                    cout << "  " << name
                         << " | heartbeat: " << info.lastHeartbeat
                         << " | bcast port: " << info.broadcastPort << endl;
            }
            cout << "=========================" << endl;
        }

        
        else if (line.rfind("broadcast ", 0) == 0)
        {
            string text = line.substr(10);
            string pkt  = "BROADCAST|" + text;

            lock_guard<mutex> lock(campusMutex);
            int sent = 0;

            for (auto &[name, info] : connectedCampuses)
            {
                // Skip if we have not yet received a heartbeat with the port
                if (!info.broadcastReady || info.broadcastPort == 0) {
                    cout << "[ADMIN] Skipping " << name
                         << " — broadcast port not yet known." << endl;
                    continue;
                }

                // Build destination: campus IP + its dedicated receive port
                sockaddr_in dest{};
                dest.sin_family = AF_INET;
                inet_pton(AF_INET, info.ipAddress.c_str(), &dest.sin_addr);
                dest.sin_port = htons(info.broadcastPort);   // ← key fix

                ssize_t r = sendto(g_udpSock, pkt.c_str(), pkt.size(), 0,
                                   (sockaddr*)&dest, sizeof(dest));
                if (r > 0) {
                    cout << "[ADMIN] Broadcast -> " << name
                         << " (port " << info.broadcastPort << ")" << endl;
                    ++sent;
                } else {
                    perror("[ADMIN] sendto failed");
                }
            }

            cout << "[ADMIN] Broadcast sent to " << sent << " campus(es)." << endl;
        }

        
        else if (line == "quit")
        {
            g_running = false;
            cout << "[ADMIN] Shutting down..." << endl;
        }

        else
        {
            cout << "[ADMIN] Unknown command. Use: list | broadcast <msg> | quit" << endl;
        }
    }
}



int main()
{
    cout << "NU Information Exchange Server (Islamabad)\n" << endl;
    cout << "TCP: " << TCP_PORT << "  UDP heartbeat: " << UDP_PORT << endl;

    
    int tcpSock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcpSock < 0) { perror("TCP socket"); return 1; }

    int opt = 1;
    setsockopt(tcpSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in tcpAddr{};
    tcpAddr.sin_family      = AF_INET;
    tcpAddr.sin_addr.s_addr = INADDR_ANY;
    tcpAddr.sin_port        = htons(TCP_PORT);

    if (bind(tcpSock, (sockaddr*)&tcpAddr, sizeof(tcpAddr)) < 0)
        { perror("TCP bind"); return 1; }
    if (listen(tcpSock, 10) < 0)
        { perror("TCP listen"); return 1; }

    cout << "[SERVER] TCP listening on port " << TCP_PORT << endl;

    
    g_udpSock = socket(AF_INET, SOCK_DGRAM, 0);
    if (g_udpSock < 0) { perror("UDP socket"); return 1; }

    setsockopt(g_udpSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in udpAddr{};
    udpAddr.sin_family      = AF_INET;
    udpAddr.sin_addr.s_addr = INADDR_ANY;
    udpAddr.sin_port        = htons(UDP_PORT);

    if (bind(g_udpSock, (sockaddr*)&udpAddr, sizeof(udpAddr)) < 0)
        { perror("UDP bind"); return 1; }

    cout << "[SERVER] UDP listening on port " << UDP_PORT << endl;

    
    thread(udpListenerThread).detach();
    thread(adminThread).detach();

    cout << "[SERVER] Waiting for campus connections...\n" << endl;

   
    while (g_running)
    {
        sockaddr_in clientAddr{};
        socklen_t   len = sizeof(clientAddr);

        int clientSock = accept(tcpSock, (sockaddr*)&clientAddr, &len);
        if (clientSock < 0) {
            if (g_running) perror("accept");
            continue;
        }

        thread(handleClient, clientSock, clientAddr).detach();
    }

    close(tcpSock);
    close(g_udpSock);
    cout << "[SERVER] Stopped." << endl;
    return 0;
}