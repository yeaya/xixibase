xixibaseDIR=$(pwd)
cmake -DCMAKE_BUILD_TYPE=Release .
#cmake .
make
mkdir dist
mkdir dist/xixibase
mkdir dist/xixibase/bin
cp bin/xixibase dist/xixibase/bin/.
mkdir dist/xixibase/conf
cp conf/web.xml dist/xixibase/conf/.
cp conf/server.xml dist/xixibase/conf/.
cp conf/key.pem dist/xixibase/conf/.
cp conf/cert.pem dist/xixibase/conf/.
mkdir dist/xixibase/webapps
cp -r -f webapps/* dist/xixibase/webapps/.
cd dist
tar cvzf xixibase-ver-os-bin.tar.gz xixibase
cd ..
