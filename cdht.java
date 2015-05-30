import java.io.*;
import java.net.*;

public class cdht {
	private static int SERVER_PORT = 5000;
	private static String SERVER_NAME = "localhost";
	private static int WAIT = 3000;

	public static void main(String[] args) throws Exception {
		int id = Integer.parseInt(args[0]);
		int s1 = Integer.parseInt(args[1]);
		int s2 = Integer.parseInt(args[2]);
		int responseId;
		// Initialise as illegal value -1 to check whether
		// predecessors have been set (Java won't take null)
		int p1 = -1;
		int p2 = -1;
		boolean s1Exists = false;
		boolean s2Exists = false;
		boolean p1Ack = false;
		boolean p2Ack = false;
		String input;
		String[] inputSplit = null;
		long lastPing = 0;
		long currTime = 0;
		
		// Create a datagram socket for receiving and sending UDP packets
		DatagramSocket socket = new DatagramSocket(SERVER_PORT + id);
		
		// Read input
		BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
			
		// Server loop
		while (true) {
			currTime = System.currentTimeMillis();
			
			if (inFromUser.ready()) {
				input = inFromUser.readLine();
				inputSplit = input.split(" ");
				int length = inputSplit.length;
				
				// Input for a file request
				if (length == 2 && inputSplit[0].equalsIgnoreCase("REQUEST")) {
					fileRequest(id, s1, inputSplit, socket);
				} else if (length == 1 && inputSplit[0].equalsIgnoreCase("QUIT")) {
					// Send departure messages to predecessors in TCP
					InetAddress address = InetAddress.getByName(SERVER_NAME);
					int port = SERVER_PORT + p1;
					byte[] buf = ("QUIT " + s1 + ' ' + s2).getBytes(); // Inform predecessors of this peer's former successors
					
					DatagramPacket quit = new DatagramPacket(buf, buf.length, address, port);
					socket.send(quit);
					
					port = SERVER_PORT + p2;
					quit = new DatagramPacket(buf, buf.length, address, port);
					socket.send(quit);
				}
			}
			
			// Keep pinging the two successors until we get a reply
			if (currTime - lastPing >= WAIT) {
				if (!s1Exists) {
					ping(socket, s1);
					lastPing = currTime;
				}
				
				if (!s2Exists) {
					ping(socket, s2);
					lastPing = currTime;
				}
			}
			
			// Create a datagram packet to hold incoming UDP packet.
			DatagramPacket request = new DatagramPacket(new byte[1024], 1024);
			
			socket.setSoTimeout(WAIT); // Set a timeout so the run loop can take input
			try {
				socket.receive(request);
			} catch(Exception e) {
				// Do nothing, we don't want to print unreceived packet exceptions
			}
			
			String[] data = new String(request.getData()).trim().split(" ");
			responseId = request.getPort() - SERVER_PORT;
			
			// Ping packets
			if (data[0].equalsIgnoreCase("PING")) {
				// Send reply
		        InetAddress address = request.getAddress();
				int port = request.getPort();
				byte[] buf = "RETURN".getBytes();
				
				System.out.println("A ping request message was received from Peer " + responseId);
				
				DatagramPacket reply = new DatagramPacket(buf, buf.length, address, port);
				socket.send(reply);
				
				// Check whether predecessors have been set
				if (p2 == -1) {
					p2 = p1;
					p1 = responseId;
				} else {
					// A predecessor must have quit
					if (responseId <= p2) {
						// p2 has quit
						p2 = responseId;
					} else {
						// p1 has quit
						p1 = responseId;
					}
				}

			} else if (data[0].equalsIgnoreCase("RETURN")) {
				// Add to successors so we no longer need to ping
				System.out.println("A ping response message was received from Peer " + responseId);
				
				if (responseId == s1) {
					s1Exists = true;
				}
				if (responseId == s2) {
					s2Exists = true;
				}
				
			// File request packets
			} else if (data[0].equalsIgnoreCase("REQUEST")) {
				String file = data[1];
				int fileKey = hash(file);
				int requestId = Integer.parseInt(data[2]);
				
				if (id == fileKey) {
					InetAddress address = InetAddress.getByName(SERVER_NAME);
					int port = SERVER_PORT + Integer.parseInt(data[2]);
					byte[] buf = ("FOUND " + file).getBytes();
					
					System.out.println("File " + file + " is here.");
					
					// Send out the response
					DatagramPacket response = new DatagramPacket(buf, buf.length, address, port);
					socket.send(response);
					
					System.out.println("A response message, destined for peer " + requestId + ", has been sent.");
				} else {
					// Either successor is closer and greater than or equal to the file
					if ((s1 >= fileKey && s1 - fileKey < Math.abs(id - fileKey)) || 
							(s2 >= fileKey && s2 - fileKey < Math.abs(id - fileKey))) {
						InetAddress address = InetAddress.getByName(SERVER_NAME);
						int port = SERVER_PORT + s1;
						byte[] buf = ("REQUEST " + file + ' ' + requestId).getBytes();
						
						System.out.println("File " + file + " is not stored here.");
						
						// Forward request to first successor
						DatagramPacket forward = new DatagramPacket(buf, buf.length, address, port);
						socket.send(forward);
						
						System.out.println("File request message has been forwarded to my successor.");
					} else {
						// Found
						System.out.println("File " + file + " is here.");
						
						InetAddress address = InetAddress.getByName(SERVER_NAME);
						int port = SERVER_PORT + requestId;
						byte[] buf = ("FOUND " + file).getBytes();
						
						// Send out the response
						DatagramPacket response = new DatagramPacket(buf, buf.length, address, port);
						socket.send(response);
						
						System.out.println("A response message, destined for peer " + requestId + ", has been sent.");
					}

				}
			
			// File found packets
			} else if (data[0].equalsIgnoreCase("FOUND")) {
				System.out.println("Received a response message from peer " + responseId + ", which has the file.");
				
			// Graceful departure request packets
			} else if (data[0].equalsIgnoreCase("QUIT")) {
				System.out.println("Peer " + responseId + " will depart from the network.");
				
				if (s1 == responseId) {
					// Current peer is first predecessor
					s1 = Integer.parseInt(data[1]);
					s2 = Integer.parseInt(data[2]);
				} else if (s2 == responseId) {
					// Second predecessor
					s2 = Integer.parseInt(data[1]);
				}
				
				
				System.out.println("My first successor is now peer " + s1);
				System.out.println("My second successor is now peer " + s2);
				
				// Start pinging new successors
				s1Exists = false;
				s2Exists = false;
				
				// Send departure ack to graceful departee
				InetAddress address = request.getAddress();
				int port = request.getPort();
				byte[] buf = "QUIT_ACK".getBytes();
				
				DatagramPacket reply = new DatagramPacket(buf, buf.length, address, port);
				socket.send(reply);
				
			// Ack of graceful departure
			} else if (data[0].equalsIgnoreCase("QUIT_ACK")) {
				if (responseId == p1) {
					p1Ack = true;
				}
				if (responseId == p2) {
					p2Ack = true;
				}

				if (p1Ack && p2Ack) {
					// Got permission from both predecessors to quit
					System.exit(0);
				}
			}
		}
	}

	
	private static void fileRequest(int requestId, int successorId, String[] inputSplit,
			DatagramSocket socket) throws UnknownHostException, IOException {
		String file = inputSplit[1];
		InetAddress address = InetAddress.getByName(SERVER_NAME);
		int port = SERVER_PORT + successorId;
		// REQUEST + fileName + requesterId
		byte[] buf = ("REQUEST " + file + ' ' + requestId).getBytes();
		
		DatagramPacket forward = new DatagramPacket(buf, buf.length, address, port);
		socket.send(forward);
		
		System.out.println("File request message for " + file + " has been sent to my successor.");
	}
	
	
	/**
	 * Convert filename to assignment spec hash value
	 * 
	 * @param file
	 * @return hashed filename
	 */
	private static int hash(String file) {
		return Integer.parseInt(file) % 256;
	}

	/**
	 * Pings a successor in the CDHT
	 * 
	 * @param socket
	 * @param successorId
	 * @return true if successor exists, false if no reply
	 * @throws Exception
	 */
	private static void ping(DatagramSocket socket, int successorId) throws Exception {
		InetAddress address = InetAddress.getByName(SERVER_NAME);
		byte[] buf = "PING".getBytes();
		DatagramPacket ping = new DatagramPacket(buf, buf.length, address, SERVER_PORT + successorId);
		
		socket.send(ping);
	}
}
