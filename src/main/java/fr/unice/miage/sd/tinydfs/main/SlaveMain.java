package fr.unice.miage.sd.tinydfs.main;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.nodes.Slave;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;


public class SlaveMain extends UnicastRemoteObject implements Slave { 
    
    private int id;
    String dfsRootFolder;
    private Slave leftSlave;
    private Slave rightSlave;
    
    // Usage: java fr.unice.miage.sd.tinydfs.main.SlaveMain master_host dfs_root_folder slave_identifier
    public static void main(String[] args) throws RemoteException,
                NotBoundException, MalformedURLException {
            String masterHost = args[0];
            String dfsRootFolder = args[1];
            int slaveId = Integer.parseInt(args[2]);

            // Create slave and register it (registration name must be "slave" + slave identifier)
            SlaveMain slaveMain = new SlaveMain(slaveId, dfsRootFolder);
            System.out.println("Slave" + slaveId + " : Créé");
            Master slaveMainStub = (Master) Naming.lookup("rmi://localhost/" + masterHost);
            
    }

    public SlaveMain(int id, String dfsRootFolder) throws RemoteException {
        super();
        this.id = id;
        this.dfsRootFolder = dfsRootFolder;
    }
    
    @Override
    public int getId() throws RemoteException {
        return this.id;
    }

    @Override
    public Slave getLeftSlave() throws RemoteException {
        return this.leftSlave;
    }

    @Override
    public Slave getRightSlave() throws RemoteException {
        return this.rightSlave;
    }

    @Override
    public void setLeftSlave(Slave slave) throws RemoteException {
        this.leftSlave = slave;
    }

    @Override
    public void setRightSlave(Slave slave) throws RemoteException {
        this.rightSlave = slave;
    }

    @Override
    public void subSave(String filename, List<byte[]> subFileContent) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<byte[]> subRetrieve(String filename) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}