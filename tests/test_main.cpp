#include "weir/weir.hpp"
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>
namespace {
void check(bool condition, const char* message) {
 if (!condition) { std::cerr << "test failure: " << message << '\n'; std::exit(1); }
}
}
int main(){
 weir::Event e{42,"payload",{},{}}; auto b=weir::encode(e); weir::Parser p;
 std::vector<std::uint8_t> golden{0x57,0x52,0x30,0x31,0,0,0,0,0,0,0,42,0,0,0,7,'p','a','y','l','o','a','d',0x33,0x0f,0x77,0xa5}; check(b==golden,"golden frame");
 for(std::size_t i=0;i<b.size();++i){weir::Parser fragmented;auto a=fragmented.feed(b.data(),i);check(a.empty(),"fragment prefix");a=fragmented.feed(b.data()+i,b.size()-i);check(a.size()==1&&a[0].id==42,"fragment decode");}
 auto bad=b; bad.back()^=1; check(p.feed(bad.data(),bad.size()).empty(),"bad checksum");
 std::vector<std::uint8_t> noise{0,1,2,3,4}; noise.insert(noise.end(),b.begin(),b.end()); auto recovered=p.feed(noise.data(),noise.size()); check(recovered.size()==1,"noise recovery");
 auto path=std::filesystem::temp_directory_path()/("weir-test-"+std::to_string(::getpid())+"-"+std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()))+"-"+std::to_string(std::rand())+".log"); std::filesystem::remove(path);
 {weir::Log l(path);check(l.append(e),"append");}
 {std::ofstream out(path,std::ios::app|std::ios::binary);out.write("WR01",4);}
 {weir::Log l(path);check(l.recover()==43,"recover id");auto records=l.replay();check(records.size()==1&&records[0].id==42,"replay first");}
 weir::BoundedQueue<int> q(1);check(q.push(7),"queue push");bool waiter_result=true;std::thread waiter([&]{waiter_result=q.push(8);});q.close();waiter.join();check(!waiter_result,"closed queue push");check(q.pop().value()==7,"queue pop");check(!q.pop().has_value(),"closed queue empty");
  {weir::Log l(path);weir::Metrics m;{weir::Pipeline pipeline(l,m,1);auto ack=std::make_shared<std::promise<bool>>();auto done=ack->get_future();check(pipeline.submit({9,"queued",ack,{}}),"pipeline submit");check(done.get(),"pipeline ack");}}
 try { weir::Log l(path); weir::Metrics m; weir::Pipeline invalid(l,m,0); check(false,"zero workers"); } catch(const std::invalid_argument&) {}
  {weir::Log l(path);check(l.recover()==43,"recover after pipeline");auto records=l.replay();check(records.size()==2&&records.back().id==9,"replay pipeline event");}
  weir::Metrics m;m.inc("events");check(m.prometheus().find("weir_events 1")!=std::string::npos,"metrics");std::filesystem::remove(path);std::cout<<"all tests passed\n";
}
