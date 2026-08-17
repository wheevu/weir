#include "weir/weir.hpp"
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
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
std::vector<Event> Parser::feed(const std::uint8_t*p,std::size_t n){
  if(bad_)return {};
  b_.insert(b_.end(),p,p+n);
  std::vector<Event>r;
  while(b_.size()>=20){
    if(get32(b_,0)!=0x57523031u){b_.erase(b_.begin());continue;}
    auto len=get32(b_,12);
    if(len>1u<<20){bad_=true;break;}
    if(b_.size()<20+len)break;
    std::string s(b_.begin()+16,b_.begin()+16+len);
    if(checksum(s)==get32(b_,16+len))r.push_back({get64(b_,4),std::move(s),{},{} });
    b_.erase(b_.begin(),b_.begin()+20+len);
  }
  return r;
}
Log::Log(std::filesystem::path p):path_(std::move(p)),out_(path_,std::ios::app|std::ios::binary){}
bool Log::append(const Event&e){auto b=encode(e);std::lock_guard l(m_);if(!out_)return false;out_.write(reinterpret_cast<const char*>(b.data()),static_cast<std::streamsize>(b.size()));out_.flush();return out_.good();}
// Read the whole log into memory with one buffered read. Reading through
// istreambuf_iterator would cost a virtual call per byte and is orders of
// magnitude slower under instrumentation.
static std::vector<std::uint8_t> read_all(const std::filesystem::path&p){
  std::ifstream in(p,std::ios::binary|std::ios::ate);
  if(!in)return {};
  auto size=in.tellg();
  if(size<0)return {};
  std::vector<std::uint8_t>b(static_cast<std::size_t>(size));
  in.seekg(0);
  if(!in.read(reinterpret_cast<char*>(b.data()),size))return {};
  return b;
}
std::uint64_t Log::recover(){std::lock_guard l(m_);auto b=read_all(path_);std::size_t off=0,good=0;std::uint64_t next=1;while(off+20<=b.size()){if(get32(b,off)!=0x57523031u){++off;continue;}auto len=get32(b,off+12);if(len>1u<<20||b.size()-off<20+len)break;std::string payload(b.begin()+static_cast<std::ptrdiff_t>(off+16),b.begin()+static_cast<std::ptrdiff_t>(off+16+len));if(checksum(payload)==get32(b,off+16+len)){auto id=get64(b,off+4);if(id==std::numeric_limits<std::uint64_t>::max())throw std::overflow_error("event id exhausted");next=std::max(next,id+1);off+=20+len;good=off;}else ++off;}std::error_code ec;std::filesystem::resize_file(path_,good,ec);out_.close();out_.open(path_,std::ios::app|std::ios::binary);return next;}
std::vector<Event> Log::replay()const{std::lock_guard l(m_);auto b=read_all(path_);std::vector<Event>r;std::size_t off=0;while(off+20<=b.size()){if(get32(b,off)!=0x57523031u){++off;continue;}auto len=get32(b,off+12);if(len>1u<<20||b.size()-off<20+len)break;std::string payload(b.begin()+static_cast<std::ptrdiff_t>(off+16),b.begin()+static_cast<std::ptrdiff_t>(off+16+len));if(checksum(payload)==get32(b,off+16+len))r.push_back({get64(b,off+4),std::move(payload),{},{} });off+=20+len;}return r;}
void Metrics::inc(std::string n){std::lock_guard l(m_);++v_[std::move(n)];}
std::string Metrics::prometheus()const{std::lock_guard l(m_);std::ostringstream o;for(auto&[k,v]:v_)o<<"weir_"<<k<<" "<<v<<"\n";return o.str();}
void log(std::string_view l,std::string_view m){auto now=std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());std::cout<<"{\"ts\":"<<now<<",\"level\":\""<<l<<"\",\"msg\":\""<<m<<"\"}\n"<<std::flush;}
Pipeline::Pipeline(Log&l,Metrics&m,unsigned n):log_(l),metrics_(m){
  if(n==0)throw std::invalid_argument("workers must be greater than zero");
  // Test-only hook: delays ACK delivery so async-lifetime races become
  // deterministic in integration tests. Inert unless the env var is set.
  std::uint64_t ack_delay_ms=0;
  if(const char* d=std::getenv("WEIR_TEST_ACK_DELAY_MS"))ack_delay_ms=std::strtoull(d,nullptr,10);
  persister_=std::thread([this,ack_delay_ms]{while(auto e=durable_.pop()){
    bool ok=log_.append(*e);
    if(ack_delay_ms>0)std::this_thread::sleep_for(std::chrono::milliseconds(ack_delay_ms));
    if(e->durable_ack)e->durable_ack->set_value(ok);
    if(e->durable_completion)e->durable_completion(ok);
    if(ok){metrics_.inc("durable_total");process_.push(std::move(*e));}
  }});
  for(unsigned i=0;i<n;++i)workers_.emplace_back([this]{while(auto e=process_.pop()){metrics_.inc("processed_total");(void)e;}});
}
Pipeline::~Pipeline(){durable_.close();if(persister_.joinable())persister_.join();process_.close();for(auto&t:workers_)if(t.joinable())t.join();}
bool Pipeline::submit(Event e){metrics_.inc("validated_total");return durable_.push(std::move(e));}
#ifdef __linux__
int run_server(unsigned port,Log&lf,Metrics&m,std::atomic<bool>&stop,unsigned workers){
  if(port==0||port>65535){return 2;}
  int s=socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK,0);
  if(s<0){return 1;}
  int yes=1;
  setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes);
  sockaddr_in a{};
  a.sin_family=AF_INET;
  a.sin_port=htons(static_cast<std::uint16_t>(port));
  a.sin_addr.s_addr=htonl(INADDR_ANY);
  if(bind(s,reinterpret_cast<sockaddr*>(&a),sizeof a)!=0||listen(s,64)!=0){close(s);return 1;}
  int ep=epoll_create1(0),wake=eventfd(0,EFD_NONBLOCK);
  if(ep<0||wake<0){if(ep>=0)close(ep);if(wake>=0)close(wake);close(s);return 1;}
  epoll_event ev{};
  ev.events=EPOLLIN;
  ev.data.fd=s;
  if(epoll_ctl(ep,EPOLL_CTL_ADD,s,&ev)!=0){
    log("error","epoll ADD listen failed: "+std::string(std::strerror(errno)));
    close(wake);close(ep);close(s);
    return 1;
  }
  epoll_event we{};
  we.events=EPOLLIN;
  we.data.fd=wake;
  if(epoll_ctl(ep,EPOLL_CTL_ADD,wake,&we)!=0){
    log("error","epoll ADD wake failed: "+std::string(std::strerror(errno)));
    close(wake);close(ep);close(s);
    return 1;
  }

  // Per-connection state, owned exclusively by the epoll thread.
  // gen is a monotonic identity that is never reused, so an async
  // persistence completion can never target a later connection that
  // happens to reuse the same numeric fd.
  struct Conn {
    Parser parser;
    std::string out;          // pending outbound ACK/ERR bytes
    std::uint64_t gen=0;
    bool read_open=true;      // peer may still send; false after EOF
    std::uint64_t pending=0;  // persistence completions outstanding
  };
  std::map<int,Conn> conns;
  std::uint64_t next_gen=0;
  std::mutex cm;
  std::deque<std::tuple<int,std::uint64_t,std::uint64_t,bool>> done;
  std::vector<epoll_event> es(32);
  std::uint64_t next=lf.recover();

  auto pipe=std::make_unique<Pipeline>(lf,m,workers);

  auto drop=[&](int fd){
    auto it=conns.find(fd);
    if(it==conns.end())return;  // exactly-once teardown
    if(epoll_ctl(ep,EPOLL_CTL_DEL,fd,nullptr)!=0){
      log("error","epoll DEL failed: "+std::string(std::strerror(errno)));
    }
    close(fd);
    conns.erase(it);
  };

  // Derive the epoll interest mask from connection state. EPOLLOUT is armed
  // only while outbound bytes are pending, so an idle or readable socket
  // cannot busy-loop the loop. EPOLLRDHUP is level-triggered and stays
  // asserted after the peer half-closes; keeping it armed would make
  // epoll_wait return instantly for every pending completion or blocked
  // write. It is therefore registered only while the read side is open.
  auto update_interest=[&](int fd){
    auto it=conns.find(fd);
    if(it==conns.end())return;
    std::uint32_t want=0;
    if(it->second.read_open)want|=EPOLLIN|EPOLLRDHUP;
    if(!it->second.out.empty())want|=EPOLLOUT;
    epoll_event ce{};
    ce.events=want;
    ce.data.fd=fd;
    if(epoll_ctl(ep,EPOLL_CTL_MOD,fd,&ce)!=0){
      log("error","epoll MOD failed: "+std::string(std::strerror(errno)));
    }
  };

  // A connection may be destroyed only when every dependency is gone:
  // the peer finished sending, no persistence completions are pending,
  // and no outbound bytes remain queued.
  auto maybe_close=[&](int fd){
    auto it=conns.find(fd);
    if(it==conns.end())return;
    const auto& c=it->second;
    if(!c.read_open&&c.pending==0&&c.out.empty())drop(fd);
  };
  while(!stop){
    int n=epoll_wait(ep,es.data(),static_cast<int>(es.size()),100);
    if(n<0){if(errno==EINTR)continue;continue;}
    for(int i=0;i<n;++i){
      int fd=static_cast<int>(es[static_cast<std::size_t>(i)].data.u64);
      std::uint32_t events=es[static_cast<std::size_t>(i)].events;
      if(fd==wake){
        std::uint64_t x;
        read(wake,&x,sizeof x);
        std::lock_guard l(cm);
        while(!done.empty()){
          auto[c,g,id,ok]=done.front();
          done.pop_front();
          auto it=conns.find(c);
          if(it==conns.end())continue;    // connection already gone
          if(it->second.gen!=g)continue;  // fd reused by another connection
          if(it->second.pending>0)--it->second.pending;
          std::string msg=ok?"OK "+std::to_string(id)+"\n":"ERR persistence\n";
          if(it->second.out.size()+msg.size()>1024*1024){drop(c);continue;}
          it->second.out+=msg;
          update_interest(c);
        }
        continue;
      }
      if(fd==s){
        for(;;){
          int c=accept4(s,nullptr,nullptr,SOCK_NONBLOCK);
          if(c<0)break;
          Conn nc;
          nc.gen=++next_gen;
          conns.emplace(c,std::move(nc));
          epoll_event ce{};
          ce.events=EPOLLIN|EPOLLRDHUP;
          ce.data.fd=c;
          if(epoll_ctl(ep,EPOLL_CTL_ADD,c,&ce)!=0){
            log("error","epoll ADD conn failed: "+std::string(std::strerror(errno)));
            close(c);
            conns.erase(c);
          }
        }
        continue;
      }
      auto it=conns.find(fd);
      if(it==conns.end())continue;
      Conn& c=it->second;
      if(events&EPOLLERR){drop(fd);continue;}
      // A peer write-half close is a state transition, not death. If readable
      // data is also pending, drain it first; recv() reports the EOF.
      if(events&(EPOLLHUP|EPOLLRDHUP)&&!(events&EPOLLIN))c.read_open=false;
      if(events&EPOLLIN){
        bool conn_gone=false;
        std::uint8_t buf[4096];
        for(;;){
          auto r=recv(fd,buf,sizeof buf,0);
          if(r==0){c.read_open=false;break;}
          if(r<0){
            if(errno==EINTR)continue;
            if(errno==EAGAIN||errno==EWOULDBLOCK)break;
            drop(fd);
            conn_gone=true;
            break;
          }
          auto parsed=c.parser.feed(buf,static_cast<std::size_t>(r));
          for(auto& e:parsed){
            e.id=next++;
            auto g=c.gen;
            ++c.pending;
            e.durable_completion=[&,fd,g,id=e.id](bool ok){
              std::lock_guard l(cm);
              done.emplace_back(fd,g,id,ok);
              std::uint64_t x=1;
              write(wake,&x,sizeof x);
            };
            if(!pipe->submit(std::move(e))){drop(fd);conn_gone=true;break;}
          }
          if(conn_gone)break;
          // A malformed frame poisons this connection only: valid events
          // decoded before it were already submitted above.
          if(c.parser.bad()){drop(fd);conn_gone=true;break;}
        }
        if(conn_gone)continue;
        if(!c.read_open)update_interest(fd);
      }
      if(!conns.count(fd))continue;
      if((events&EPOLLOUT)&&!c.out.empty()){
        auto r=send(fd,c.out.data(),c.out.size(),MSG_NOSIGNAL);
        if(r>0){
          c.out.erase(0,static_cast<std::size_t>(r));
        }else if(r<0&&errno!=EAGAIN&&errno!=EWOULDBLOCK&&errno!=EINTR){
          drop(fd);
          continue;
        }
        update_interest(fd);
      }
      if(!conns.count(fd))continue;
      maybe_close(fd);
    }
  }
  // Stop Pipeline callbacks before closing the wake fd they signal.
  pipe.reset();
  // Close every client socket explicitly; the epoll registrations die with
  // the epoll instance.
  for(auto& kv:conns)close(kv.first);
  conns.clear();
  close(wake);close(ep);close(s);
  return 0;
}
int run_metrics_http(unsigned port,Metrics&m,std::atomic<bool>&stop){
  if(port==0||port>65535){return 2;}
  int s=socket(AF_INET,SOCK_STREAM,0);
  if(s<0){log("error","metrics socket failed: "+std::string(std::strerror(errno)));return 1;}
  int yes=1;
  setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes);
  sockaddr_in a{};
  a.sin_family=AF_INET;
  a.sin_port=htons(static_cast<std::uint16_t>(port));
  a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
  if(bind(s,reinterpret_cast<sockaddr*>(&a),sizeof a)!=0){
    log("error","metrics bind failed: "+std::string(std::strerror(errno)));
    close(s);
    return 1;
  }
  if(listen(s,8)!=0){
    log("error","metrics listen failed: "+std::string(std::strerror(errno)));
    close(s);
    return 1;
  }
  pollfd p{s,POLLIN,0};
  while(!stop){
    if(poll(&p,1,100)<1)continue;
    int c=accept(s,nullptr,nullptr);
    if(c<0)continue;
    // A silent metrics client must not block shutdown: bound the request
    // read so the loop can always proceed to respond and close.
    timeval tv{1,0};
    setsockopt(c,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof tv);
    std::uint8_t tmp[4096];
    (void)recv(c,tmp,sizeof tmp,0);
    auto body=m.prometheus();
    std::string r="HTTP/1.1 200 OK\r\nContent-Length: "+std::to_string(body.size())+"\r\nConnection: close\r\n\r\n"+body;
    send(c,r.data(),r.size(),MSG_NOSIGNAL);
    shutdown(c,SHUT_WR);
    close(c);
  }
  close(s);
  return 0;
}
#else
int run_server(unsigned,Log&,Metrics&,std::atomic<bool>&,unsigned){log("warn","network server unavailable on this platform");return 2;}int run_metrics_http(unsigned,Metrics&,std::atomic<bool>&){return 2;}
#endif
}
