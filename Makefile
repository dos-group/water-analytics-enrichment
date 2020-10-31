all: complete

clean:
	mvn clean

compile:
	mvn compile

package:
	mvn package

complete:
	mvn clean compile package
