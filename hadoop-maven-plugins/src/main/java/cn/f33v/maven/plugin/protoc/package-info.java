/**
 * 给予protoc命令基于协议缓冲区生成java文件的插件
 * 对于用于主要构建工件的生成文件，请使用以下命令：
 * <plugins>
 *       ... SNIP ...
 *       <plugin>
 *         <groupId>org.apache.hadoop</groupId>
 *         <artifactId>hadoop-maven-plugins</artifactId>
 *         <executions>
 *           ... SNIP ...
 *           <execution>
 *             <id>compile-protoc</id>
 *             <goals>
 *               <goal>protoc</goal>
 *             </goals>
 *             <configuration>
 *               <protocVersion>${protobuf.version}</protocVersion>
 *               <protocCommand>${protoc.path}</protocCommand>
 *               <imports>
 *                 <param>${basedir}/src/main/proto</param>
 *               </imports>
 *               <source>
 *                 <directory>${basedir}/src/main/proto</directory>
 *                 <includes>
 *                   <include>HAServiceProtocol.proto</include>
 *                   ... SNIP ...
 *                   <include>RefreshCallQueueProtocol.proto</include>
 *                   <include>GenericRefreshProtocol.proto</include>
 *                 </includes>
 *               </source>
 *             </configuration>
 *           </execution>
 *           ... SNIP ...
 *         </executions>
 *         ... SNIP ...
 *       </plugin>
 *     </plugins>
 *
 * 对于仅用于测试的生成文件，请使用
 * <plugins>
 *       ... SNIP ...
 *       <plugin>
 *         <groupId>org.apache.hadoop</groupId>
 *         <artifactId>hadoop-maven-plugins</artifactId>
 *         <executions>
 *           ... SNIP ...
 *           <execution>
 *             <id>compile-test-protoc</id>
 *             <goals>
 *               <goal>test-protoc</goal>
 *             </goals>
 *             <configuration>
 *               <protocVersion>${protobuf.version}</protocVersion>
 *               <protocCommand>${protoc.path}</protocCommand>
 *               <imports>
 *                 <param>${basedir}/src/test/proto</param>
 *               </imports>
 *               <source>
 *                 <directory>${basedir}/src/test/proto</directory>
 *                 <includes>
 *                   <include>test.proto</include>
 *                   <include>test_rpc_service.proto</include>
 *                 </includes>
 *               </source>
 *             </configuration>
 *           </execution>
 *           ... SNIP ...
 *         </executions>
 *         ... SNIP ...
 *       </plugin>
 *     </plugins>
 *
 */

package cn.f33v.maven.plugin.protoc;
