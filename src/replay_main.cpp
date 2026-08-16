#include "weir/weir.hpp"
#include <iostream>
int main(int argc,char**argv){if(argc!=2){std::cerr<<"usage: weir-replay LOG\n";return 2;}weir::Log l(argv[1]);for(auto&e:l.replay())std::cout<<e.id<<"\t"<<e.payload<<"\n";return 0;}
