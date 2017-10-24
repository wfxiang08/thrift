./bootstrap.sh
export CXXFLAGS="-std=c++11"
./configure --prefix=/usr/local/ --with-boost=/usr/local --without-libevent --without-ruby --without-haskell --without-erlang --without-perl --without-nodejs --without-cpp --without-json --without-as3 --without-csharp --without-erl --without-cocoa --without-ocaml --without-hs --without-xsd --without-html --without-delphi --without-gv --without-lua --without-c --without-c_glib --without-php_extension --without-d --without-openssl --without-py3 --without-python
make CXXFLAGS=-stdlib=libstdc++ -j 8 &&  sudo make install
