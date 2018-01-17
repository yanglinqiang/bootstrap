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
        String script =
                "df=pd.read_json('%s')\n" +
                        "df['%s']=pd.Series([str(uuid.uuid3()) for x in df.index])\n" +
                        "df.to_json('%s')\n";
        script=String.format(script,"/tmp/dss/cars.json","uuid","/tmp/dss/cars1.json");
        try {
            System.out.println(fastExecuteClient.start(script));
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
