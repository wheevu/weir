#include "weir/weir.hpp"
#include "weir/coroutine_server.hpp"
#include <atomic>
#include <csignal>
#include <iostream>
#include <limits>
#include <thread>
namespace { std::atomic<bool>* stopping; void handle_signal(int){if(stopping)*stopping=true;}
bool parse_uint(const char* text,unsigned& value){try{std::size_t end=0;auto n=std::stoull(text,&end,10);if(end==0||text[end]!='\0'||n>std::numeric_limits<unsigned>::max())return false;value=static_cast<unsigned>(n);return true;}catch(const std::exception&){return false;}}
}
int main(int argc,char**argv){unsigned port=9000,metrics_port=9100,workers=2;std::string path="weir.log",backend="epoll";for(int i=1;i<argc;++i){std::string a=argv[i];if((a=="--port"||a=="--metrics-port"||a=="--workers"||a=="--log"||a=="--backend")&&i+1>=argc){std::cerr<<"missing option value\n";return 2;}if(a=="--help"){std::cout<<"usage: weir-server [--port N] [--metrics-port N] [--workers N] [--log PATH] [--backend epoll|coroutine|io_uring]\n";return 0;}if(a=="--log"){path=argv[++i];continue;}if(a=="--backend"){backend=argv[++i];continue;}unsigned* target=nullptr;if(a=="--port")target=&port;else if(a=="--metrics-port")target=&metrics_port;else if(a=="--workers")target=&workers;else{std::cerr<<"unknown option: "<<a<<"\n";return 2;}if(!parse_uint(argv[++i],*target)){std::cerr<<"invalid numeric option\n";return 2;}}if(workers==0||port==0||port>65535||metrics_port==0||metrics_port>65535||(backend!="epoll"&&backend!="coroutine"&&backend!="io_uring")){std::cerr<<"invalid server options\n";return 2;}std::atomic<bool>stop{false};stopping=&stop;std::signal(SIGINT,handle_signal);std::signal(SIGTERM,handle_signal);weir::Log l(path);weir::Metrics m;std::thread metrics([&]{weir::run_metrics_http(metrics_port,m,stop);});int rc=backend=="coroutine"?weir::run_coroutine_server(port,l,m,stop,workers):backend=="io_uring"?weir::run_uring_server(port,l,m,stop,workers):weir::run_server(port,l,m,stop,workers);stop=true;metrics.join();return rc;}
