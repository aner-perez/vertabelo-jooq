Vertabelo Plugin for jOOQ

To generate java code from your Vertabelo model you will need to add the jOOQ code generation plugin to your maven pom.xml and you will need to configure it to reference your Vertabelo model.  You can either specify an XML file which contains your exported Vertabelo model or you can fetch the model directly from vertabelo.com using their API.

For a local xml file you can use the following properties:

Property | Description
-------- | -----------
**xml-file** | path to your Vertabelo model XML file

To access your model directly from Vertabelo's site you can use the following properties:

Property | Description
-------- | -----------
**api-token** | API Token for your account on vertabelo.com
**model-id** | The model id for the model in your account
**tag-name** | Optional tag name for a particular version of the model

    <properties>
        <jooq.version>3.9.1</jooq.version>
    </properties>

            <plugin>
                <groupId>org.jooq</groupId>
                <artifactId>jooq-codegen-maven</artifactId>
                <version>${jooq.version}</version>
                <dependencies>
                    <dependency>
                        <groupId>org.jooq</groupId>
                        <artifactId>jooq-meta-extensions</artifactId>
                        <version>${jooq.version}</version>
                    </dependency>
                    <dependency>
                        <groupId>com.vertabelo</groupId>
                        <artifactId>vertabelo-jooq</artifactId>
                        <version>1.0.1</version>
                    </dependency>
                </dependencies>
                <executions>
                    <execution>
                        <id>generate-postgres</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>generate</goal>
                        </goals>
                        <configuration>
                            <generator>
                                <database>
                                    <name>com.vertabelo.jooq.v2_3.VertabeloDatabase</name>
                                    <properties>
                                        <property>
                                            <key>dialect</key>
                                            <value>POSTGRES</value>
                                        </property>
                                        <property>
                                            <key>xml-file</key>
                                            <value>src/main/sql/vertabelo-model.xml</value>
                                        </property>
                                    </properties>
                                </database>
                                <generate>
                                    <jpaAnnotations>true</jpaAnnotations>
                                    <validationAnnotations>true</validationAnnotations>
                                </generate>
                                <target>
                                    <packageName>com.yourcompany.jooq</packageName>
                                    <directory>target/generated-sources/jooq-postgres</directory>
                                </target>
                            </generator>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
