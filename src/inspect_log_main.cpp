#include "weir/weir.hpp"
#include <iostream>
int main(int argc,char**argv){if(argc!=2){std::cerr<<"usage: weir-inspect-log LOG\n";return 2;}weir::Log l(argv[1]);auto e=l.replay();std::cout<<"records="<<e.size()<<"\n";for(auto&x:e)std::cout<<x.id<<" bytes="<<x.payload.size()<<" checksum="<<weir::checksum(x.payload)<<"\n";return 0;}
