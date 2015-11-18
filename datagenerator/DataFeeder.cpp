// DataFeeder.cpp : Defines the entry point for the console application.
//
#include <stdlib.h>
#include <string>
#include <iostream>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<sys/time.h>
#include<netinet/in.h>
#include<stdio.h>
#include <time.h>
#include <strings.h>
#include<string.h>
#include "LRDataProvider.h"

using namespace std;

void ErrorHandler(int nErrorCode) {
	switch (nErrorCode) {
	case END_OF_FILE: {
		cout << "End of data file" << endl;
	}
		break;
	case ERROR_FILE_NOT_FOUND: {
		cout << "Data file not found. Check data file path name." << endl;
	}
		break;
	case ERROR_INVALID_FILE: {
		cout << "Invalid file handler. Restart the system." << endl;
	}
		break;
	case ERROR_BUFFER_OVERFLOW: {
		cout << "Buffer over flow. Increase the buffer size." << endl;
	}
		break;
	default: {
		cout << "Programming error." << endl;
	}
		break;
	}
}

int init_connection_server(int portno) {
	int sockfd;
	struct sockaddr_in serv_addr;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("ERROR opening socket");
		return -1;
	}
	bzero((char*) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(portno);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
		perror("ERROR on binding");
		return -1;
	}
	listen(sockfd, 10);
	return sockfd;
}

int main(int argc, char* argv[]) {
	pthread_mutex_t mutex_lock = PTHREAD_MUTEX_INITIALIZER;
	socklen_t clilen;
	struct sockaddr_in cli_addr;
	int newsockfd = 0;
	int senttuples = 0;
	// Check parameter
	if (argc < 2) {
		cout << "You have to provider input data file name as a parameter."
				<< endl;
		return 0;

	}
	int port = 9000;
	char* dataFile = argv[1];

	//CLRDataProvider
	CLRDataProvider* provider = new CLRDataProvider();

	//Initialize the provider
	cout << "Initializing..." << endl;
	int ret = provider->Initialize(dataFile, 1000, &mutex_lock);

	//Allocate caller's buffer
	if (ret != SUCCESS) {
		ErrorHandler(ret);
		return 0;

	}

	int sockfd = init_connection_server(port);
	if (sockfd < 0) {
		provider->Uninitialize();
		delete provider;
		return -1;
	}

	cout << "Waiting for connection...."<<endl;
	newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
	if (newsockfd < 0) {
		perror("ERROR on accept");
		provider->Uninitialize();
		delete provider;
		return -1;
	}
	cout << "Connection established. Sending data " << endl;
	//Using the provider
	if (provider->PrepareData(provider) != SUCCESS) {
		close(newsockfd);
		close(sockfd);
		cout << "Uninitialize..." << endl;
		provider->Uninitialize();
		delete provider;
		return -1;
	}

	int nTuplesRead = 0;
	int nMaxTuples = 100;
	LPTuple lpTuples = new Tuple[nMaxTuples];
	char* str = new char[1024];
	int seconds = 1;
	for (;;) {
		// Sleep s seconds
		sleep(seconds);
		int ret;

		for (;;) {
			//Gets available data
			ret = provider->GetData(lpTuples, nMaxTuples, nTuplesRead);

			if (ret < 0) {
				//Handle errors including eof
				ErrorHandler(ret);
				break;
			}

			if (nTuplesRead == 0) {
				//No tuple available
				//cout << "No buffer is available!" << endl;
				break;
			}

			//Using the return data
			struct timeval start;
			gettimeofday(&start, NULL);
		        memset(str, '\0', 1024);
			for (int i = 0; i < nTuplesRead; i++) {
				if (lpTuples[i].m_iType != 0)
				    continue;
				senttuples++;
				memset(str, '\0', 1024);
				sprintf(str, "%d,%ld,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                				 lpTuples[i].m_iType, (start.tv_sec) * 1000 + (start.tv_usec) / 1000,  lpTuples[i].m_iVid,
                				 lpTuples[i].m_iSpeed,  lpTuples[i].m_iXway,  lpTuples[i].m_iLane,
                 				 lpTuples[i].m_iDir,  lpTuples[i].m_iSeg,  lpTuples[i].m_iPos,
                 				 lpTuples[i].m_iQid,  lpTuples[i].m_iSinit,  lpTuples[i].m_iSend,
                 				 lpTuples[i].m_iDow,  lpTuples[i].m_iTod,  lpTuples[i].m_iDay);
				// send on the network
				int n = write(newsockfd, str, strlen(str));
				if (n < 0) {
					cout <<"ERROR writing to socket" << endl;
				}
				//cout << "Sent " << str << endl;
			}
			//cout << "End of current tuples" <<endl;

			if (nTuplesRead < nMaxTuples) {
				//Last tuple has been read
				//cout << "Read of last tuple!" << endl;
				break;
			}
		}

		if (ret < SUCCESS) {
			break;
		}
	}
	cout << "Sent tuples: "<< senttuples << endl;
	//Uninitialize the provider
	cout << "Uninitialize..." << endl;
	provider->Uninitialize();

	delete provider;

	close(newsockfd);
	close(sockfd);

	return 0;
}

