#include "weir/weir.hpp"
#include <fstream>
#include <iostream>
#ifdef __linux__
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
int main(int argc,char**argv){
 if(argc<3){std::cerr<<"usage: weir-producer HOST PORT [PAYLOAD]\n";return 2;}
#ifdef __linux__
 std::string p=argc>3?argv[3]:"hello";try{auto port=std::stoul(argv[2]);if(port>65535)return 2;int s=socket(AF_INET,SOCK_STREAM,0);if(s<0)return 1;sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(static_cast<std::uint16_t>(port));if(inet_pton(AF_INET,argv[1],&a.sin_addr)!=1||connect(s,reinterpret_cast<sockaddr*>(&a),sizeof a)){close(s);return 1;}auto b=weir::encode({1,p,{}, {}});for(std::size_t o=0;o<b.size();){auto n=send(s,b.data()+o,b.size()-o,0);if(n<=0){close(s);return 1;}o+=static_cast<std::size_t>(n);}char x[32]{};auto n=recv(s,x,sizeof x,0);if(n>0)std::cout.write(x,n);close(s);return n>0?0:1;}catch(const std::exception&){std::cerr<<"invalid port\n";return 2;}
#else
 (void)argv;
 return 2;
#endif
}
