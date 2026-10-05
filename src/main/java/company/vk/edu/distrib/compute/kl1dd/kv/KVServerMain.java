package company.vk.edu.distrib.compute.kl1dd.kv;

import company.vk.edu.distrib.compute.kv.KVService;

public class KVServerMain {
    public static void main(String[] args) throws Exception {
        KVService service = new KVServiceFactory().create(8080);
        service.start();
    }
}