package leader_election.leader_election;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher{
	
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;
	
    public static void main( String[] args ) throws IOException, InterruptedException {
    	LeaderElection app = new LeaderElection();
    	app.connectToZookeeper();
    	app.run();
    	app.close();
    	System.out.println("Disconnected from Zookeeper, exiting application");
    }
        
    public void connectToZookeeper() throws IOException {
    	this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }
    
    //주키퍼는 이벤트 기반으로 동작하는데 주키퍼가 우리 프로그램에 응답하고 다른 스레드에서 이벤트를
    //트리거하기도 전에 프로그램이 종료됨
    // 그래서 메인 스레드에서 호출할 수 있도록 run() 메서드를 만들고 그 안에
    //메인 스레드를 대기 상태로 둠
    public void run() throws InterruptedException {
    	synchronized (zooKeeper) {
    		zooKeeper.wait();
		}
    }
    
    public void close() throws InterruptedException {
    	zooKeeper.close();
    }


    //주키퍼 서버에서 새로운 이벤트가 발생할 때 마다 별도의 스레드에 있는
    //주키퍼 라이브러리에서 호출됨. 어떤 이벤트인지 먼저 파악함
	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
				if(event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("Successfully connected to Zookeeper");
				}else {
					//주키퍼 서버가 내려가는 등 연결이 끊겼을 경우
					synchronized (zooKeeper) {
						//이벤트 핸들링 스레드에서 받아서 주키퍼로부터 연결 해제 이벤트를 받았다는 메시지를 출력하고
						//메인 스레드를 깨움
						System.out.println("Disconnected from Zookeeper event");
						zooKeeper.notifyAll();
					}
				}
			break;

		default:
			break;
		}
	}
}
