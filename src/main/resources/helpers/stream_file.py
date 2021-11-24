import socket
import sys




if __name__=="__main__":
	# Main application 
	fileName = sys.argv[1]
	with open(fileName) as f:
		socketServer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		socketServer.bind(("127.0.0.1",9090));
		socketServer.listen(1);
		while True:
			try:
				(clientConnected, clientAddress) = socketServer.accept();
				line = f.readline()
				while line:
					print(line)
					clientConnected.send(line.encode())
					line = f.readline()
			except:
				pass
