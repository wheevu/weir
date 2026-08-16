#include "weir/weir.hpp"
#include <chrono>
#include <cerrno>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#ifdef __linux__
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace weir {
std::uint32_t checksum(std::string_view s){std::uint32_t c=2166136261u;for(char x:s)c=(c^static_cast<std::uint8_t>(x))*16777619u;return c;}
static void put32(std::vector<std::uint8_t>&b,std::uint32_t x){for(int i=3;i>=0;--i)b.push_back(static_cast<std::uint8_t>(x>>(i*8)));}
static void put64(std::vector<std::uint8_t>&b,std::uint64_t x){for(int i=7;i>=0;--i)b.push_back(static_cast<std::uint8_t>(x>>(i*8)));}
static std::uint32_t get32(const std::vector<std::uint8_t>&b,std::size_t p){return (static_cast<std::uint32_t>(b[p])<<24u)|(static_cast<std::uint32_t>(b[p+1])<<16u)|(static_cast<std::uint32_t>(b[p+2])<<8u)|b[p+3];}
static std::uint64_t get64(const std::vector<std::uint8_t>&b,std::size_t p){std::uint64_t x=0;for(std::size_t i=0;i<8;++i)x=(x<<8u)|b[p+i];return x;}
std::vector<std::uint8_t> encode(const Event&e){std::vector<std::uint8_t>b;put32(b,0x57523031u);put64(b,e.id);put32(b,static_cast<std::uint32_t>(e.payload.size()));b.insert(b.end(),e.payload.begin(),e.payload.end());put32(b,checksum(e.payload));return b;}
std::vector<Event> Parser::feed(const std::uint8_t*p,std::size_t n){b_.insert(b_.end(),p,p+n);std::vector<Event>r;while(b_.size()>=20){if(get32(b_,0)!=0x57523031u){b_.erase(b_.begin());continue;}auto len=get32(b_,12);if(len>1u<<20)throw std::runtime_error("frame too large");if(b_.size()<20+len)break;std::string s(b_.begin()+16,b_.begin()+16+len);if(checksum(s)==get32(b_,16+len))r.push_back({get64(b_,4),std::move(s),{},{} });b_.erase(b_.begin(),b_.begin()+20+len);}return r;}
Log::Log(std::filesystem::path p):path_(std::move(p)),out_(path_,std::ios::app|std::ios::binary){}
bool Log::append(const Event&e){auto b=encode(e);std::lock_guard l(m_);if(!out_)return false;out_.write(reinterpret_cast<const char*>(b.data()),static_cast<std::streamsize>(b.size()));out_.flush();return out_.good();}
std::uint64_t Log::recover(){std::lock_guard l(m_);std::ifstream in(path_,std::ios::binary);std::vector<std::uint8_t>b((std::istreambuf_iterator<char>(in)),{});std::size_t off=0,good=0;std::uint64_t next=1;while(off+20<=b.size()){if(get32(b,off)!=0x57523031u){++off;continue;}auto len=get32(b,off+12);if(len>1u<<20||b.size()-off<20+len)break;std::string payload(b.begin()+static_cast<std::ptrdiff_t>(off+16),b.begin()+static_cast<std::ptrdiff_t>(off+16+len));if(checksum(payload)==get32(b,off+16+len)){auto id=get64(b,off+4);if(id==std::numeric_limits<std::uint64_t>::max())throw std::overflow_error("event id exhausted");next=std::max(next,id+1);off+=20+len;good=off;}else ++off;}in.close();std::error_code ec;std::filesystem::resize_file(path_,good,ec);out_.close();out_.open(path_,std::ios::app|std::ios::binary);return next;}
std::vector<Event> Log::replay()const{std::lock_guard l(m_);std::ifstream in(path_,std::ios::binary);std::vector<std::uint8_t>b((std::istreambuf_iterator<char>(in)),{});std::vector<Event>r;std::size_t off=0;while(off+20<=b.size()){if(get32(b,off)!=0x57523031u){++off;continue;}auto len=get32(b,off+12);if(len>1u<<20||b.size()-off<20+len)break;std::string payload(b.begin()+static_cast<std::ptrdiff_t>(off+16),b.begin()+static_cast<std::ptrdiff_t>(off+16+len));if(checksum(payload)==get32(b,off+16+len))r.push_back({get64(b,off+4),std::move(payload),{},{} });off+=20+len;}return r;}
void Metrics::inc(std::string n){std::lock_guard l(m_);++v_[std::move(n)];}
std::string Metrics::prometheus()const{std::lock_guard l(m_);std::ostringstream o;for(auto&[k,v]:v_)o<<"weir_"<<k<<" "<<v<<"\n";return o.str();}
void log(std::string_view l,std::string_view m){auto now=std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());std::cout<<"{\"ts\":"<<now<<",\"level\":\""<<l<<"\",\"msg\":\""<<m<<"\"}\n"<<std::flush;}
Pipeline::Pipeline(Log&l,Metrics&m,unsigned n):log_(l),metrics_(m){if(n==0)throw std::invalid_argument("workers must be greater than zero");persister_=std::thread([this]{while(auto e=durable_.pop()){bool ok=log_.append(*e);if(e->durable_ack)e->durable_ack->set_value(ok);if(e->durable_completion)e->durable_completion(ok);if(ok){metrics_.inc("durable_total");process_.push(std::move(*e));}}});for(unsigned i=0;i<n;++i)workers_.emplace_back([this]{while(auto e=process_.pop()){metrics_.inc("processed_total");(void)e;}});}
Pipeline::~Pipeline(){durable_.close();if(persister_.joinable())persister_.join();process_.close();for(auto&t:workers_)if(t.joinable())t.join();}
bool Pipeline::submit(Event e){metrics_.inc("validated_total");return durable_.push(std::move(e));}
#ifdef __linux__
int run_server(unsigned port,Log&lf,Metrics&m,std::atomic<bool>&stop,unsigned workers){
 if(port==0||port>65535){return 2;}int s=socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK,0);if(s<0){return 1;}int yes=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(static_cast<std::uint16_t>(port));a.sin_addr.s_addr=INADDR_ANY;if(bind(s,reinterpret_cast<sockaddr*>(&a),sizeof a)||listen(s,64)){close(s);return 1;}int ep=epoll_create1(0),wake=eventfd(0,EFD_NONBLOCK);if(ep<0||wake<0){if(ep>=0)close(ep);if(wake>=0)close(wake);close(s);return 1;}
 epoll_event ev{};ev.events=EPOLLIN;ev.data.fd=s;epoll_ctl(ep,EPOLL_CTL_ADD,s,&ev);epoll_event we{};we.events=EPOLLIN;we.data.fd=wake;epoll_ctl(ep,EPOLL_CTL_ADD,wake,&we);
  std::map<int,Parser>ps;std::map<int,std::string>out;std::map<int,std::uint64_t>gen,next_gen;std::mutex cm;std::deque<std::tuple<int,std::uint64_t,std::uint64_t,bool>>done;std::vector<epoll_event>es(32);std::uint64_t next=lf.recover();
  auto drop=[&](int fd){epoll_ctl(ep,EPOLL_CTL_DEL,fd,nullptr);close(fd);ps.erase(fd);out.erase(fd);gen.erase(fd);};
  auto pipe=std::make_unique<Pipeline>(lf,m,workers);
  while(!stop){int n=epoll_wait(ep,es.data(),static_cast<int>(es.size()),100);if(n<0&&errno==EINTR)continue;for(int i=0;i<n;++i){int fd=static_cast<int>(es[static_cast<std::size_t>(i)].data.u64);if(fd==wake){std::uint64_t x;read(wake,&x,sizeof x);std::lock_guard l(cm);while(!done.empty()){auto[c,g,id,ok]=done.front();done.pop_front();auto active=gen.find(c);if(active!=gen.end()&&active->second==g&&out.find(c)!=out.end()){std::string msg=ok?"OK "+std::to_string(id)+"\n":"ERR persistence\n";if(out[c].size()+msg.size()>1024*1024){drop(c);continue;}out[c]+=msg;epoll_event ce{};ce.events=EPOLLIN|EPOLLOUT|EPOLLRDHUP;ce.data.fd=c;epoll_ctl(ep,EPOLL_CTL_MOD,c,&ce);}}}else if(fd==s){for(;;){int c=accept4(s,nullptr,nullptr,SOCK_NONBLOCK);if(c<0)break;gen[c]=++next_gen[c];ps.emplace(c,Parser{});epoll_event ce{};ce.events=EPOLLIN|EPOLLRDHUP;ce.data.fd=c;epoll_ctl(ep,EPOLL_CTL_ADD,c,&ce);}}else if(gen.count(fd)){bool dead=(es[static_cast<std::size_t>(i)].events&(EPOLLERR|EPOLLHUP|EPOLLRDHUP))!=0;if(!dead&&(es[static_cast<std::size_t>(i)].events&EPOLLIN)){try{std::uint8_t buf[4096];for(;;){auto r=recv(fd,buf,sizeof buf,0);if(r<=0)break;for(auto&e:ps[fd].feed(buf,static_cast<std::size_t>(r))){e.id=next++;auto g=gen[fd];e.durable_completion=[&,fd,g,id=e.id](bool ok){std::lock_guard l(cm);done.emplace_back(fd,g,id,ok);std::uint64_t x=1;write(wake,&x,sizeof x);};if(!pipe->submit(std::move(e)))drop(fd);}}}catch(const std::exception&){drop(fd);}if(!gen.count(fd))continue;}if(!dead&&(es[static_cast<std::size_t>(i)].events&EPOLLOUT)){auto&w=out[fd];auto r=send(fd,w.data(),w.size(),MSG_NOSIGNAL);if(r>0)w.erase(0,static_cast<std::size_t>(r));if(w.empty()){epoll_event ce{};ce.events=EPOLLIN|EPOLLRDHUP;ce.data.fd=fd;epoll_ctl(ep,EPOLL_CTL_MOD,fd,&ce);}}if(dead||(!out[fd].empty()&&send(fd,"",0,MSG_NOSIGNAL)<0&&errno==EPIPE))drop(fd);}}}
  // Stop Pipeline callbacks before closing the wake fd they signal.
  pipe.reset();
  close(wake);close(ep);close(s);
  return 0;
}
int run_metrics_http(unsigned port,Metrics&m,std::atomic<bool>&stop){if(port==0||port>65535)return 2;int s=socket(AF_INET,SOCK_STREAM,0);if(s<0)return 1;int yes=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(static_cast<std::uint16_t>(port));a.sin_addr.s_addr=INADDR_LOOPBACK;if(bind(s,reinterpret_cast<sockaddr*>(&a),sizeof a)||listen(s,8)){close(s);return 1;}pollfd p{s,POLLIN,0};while(!stop){if(poll(&p,1,100)<1)continue;int c=accept(s,nullptr,nullptr);if(c<0)continue;auto body=m.prometheus();std::string r="HTTP/1.1 200 OK\r\nContent-Length: "+std::to_string(body.size())+"\r\nConnection: close\r\n\r\n"+body;send(c,r.data(),r.size(),MSG_NOSIGNAL);close(c);}close(s);return 0;}
#else
int run_server(unsigned,Log&,Metrics&,std::atomic<bool>&,unsigned){log("warn","network server unavailable on this platform");return 2;}int run_metrics_http(unsigned,Metrics&,std::atomic<bool>&){return 2;}
#endif
}
