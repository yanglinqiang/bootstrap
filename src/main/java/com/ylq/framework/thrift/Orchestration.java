package com.ylq.framework.thrift;

import com.ylq.framework.AbstractAppWait;
import com.ylq.framework.ILoader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by ylq on 2018/1/12.
 */
public class Orchestration extends AbstractAppWait implements ILoader {

    private static TTransport transport;
    private static FastExecute.Client fastExecuteClient;

    @Override
    public void init() {
        transport = new TSocket("127.0.0.1", 8989);
        TProtocol protocol = new TBinaryProtocol(transport);
        fastExecuteClient = new FastExecute.Client(protocol);
        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
            System.out.println("Thrift Client Init Failure, Please Check Thrift Server... ");
        }
    }

    @Override
    public void start() {
        Long start = System.currentTimeMillis();
        try {
            System.out.println(fastExecuteClient.start("Hello Thrift"));
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - start));
        shutDown();
    }

    @Override
    protected void shutDown() {
        try {
            transport.close();
        } catch (Exception ex) {
        }
    }
}
