package fr.unice.miage.sd.tinydfs.main;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.nodes.Slave;
import java.io.File;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class MasterMain extends UnicastRemoteObject implements Master {
    
    private String dfsRootFolder;
    private int nbSlaves;
    private Slave leftSlave;
    private Slave rightSlave;
    
    // Usage: java fr.unice.miage.sd.tinydfs.main.MasterMain storage_service_name dfs_root_folder nb_slaves
    public static void main(String[] args) throws RemoteException,
                AlreadyBoundException, NotBoundException, MalformedURLException {
            String storageServiceName = args[0];
            String dfsRootFolder = args[1];
            int nbSlaves = Integer.parseInt(args[2]);

            // Create master and register it
            int rmiRegistryPort = 1099;
            Registry registry = LocateRegistry.createRegistry(rmiRegistryPort);
            System.out.println("aaMaster : RMI registry listening on port " + rmiRegistryPort);
            
            MasterMain masterMain = new MasterMain(dfsRootFolder,nbSlaves);
            registry.bind(storageServiceName, masterMain);
            
            String[] param = new String[3];
            param[0] = storageServiceName;
            param[1] = ".";
            System.out.println("Master : Création des " + nbSlaves + " slaves");
            for (int i = 2; i < nbSlaves+2; i++) {
                param[2] = i + "";
                SlaveMain.main(param);
            }
            
    }

    public MasterMain(String dfsRootFolder, int nbSlaves) throws RemoteException {
        super();
        this.dfsRootFolder = dfsRootFolder;
        this.nbSlaves = nbSlaves;
    }
    
    @Override
    public String getDfsRootFolder() throws RemoteException {
        return this.dfsRootFolder;
    }

    @Override
    public int getNbSlaves() throws RemoteException {
        return this.nbSlaves;
    }

    @Override
    public void saveFile(File file) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void saveBytes(String filename, byte[] fileContent) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public File retrieveFile(String filename) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] retrieveBytes(String filename) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
	
}
