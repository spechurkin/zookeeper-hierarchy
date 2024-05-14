package me.test_task;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final ZooKeeper zooKeeper;

    static {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 15000, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        // Создание нод
        zooKeeper.create("/node", "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/node/child", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/node/child1", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/node/child1/child12", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/node/child2", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/node/child2/child21", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // Запрос на вывод всех дочерних нод из корневой папки
        zooKeeper.getChildren("/", false).forEach(child -> {
            try {
                LOG.info(getChildType(child));
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
            try {
                stepIn(child, "");
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        });

        // Удаление всех нод
        zooKeeper.delete("/node/child2/child21", -1);
        zooKeeper.delete("/node/child2", -1);
        zooKeeper.delete("/node/child1/child12", -1);
        zooKeeper.delete("/node/child1", -1);
        zooKeeper.delete("/node/child", -1);
        zooKeeper.delete("/node", -1);
    }

    // Метод для присвоения типа ноды (Папка или файл). Требование двух переменных обусловлено моим нежеланием в выводе видеть полный путь до ноды
    private static String getChildType(String path, String childName) throws InterruptedException, KeeperException {
        String childWithType = childName;
        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            childWithType += " [Folder]";
        } else {
            childWithType += " [File]";
        }
        return childWithType;
    }

    // Метод для присвоения типа ноды (Папка или файл). Вариант на случай необходимости вывода полного пути до ноды
    private static String getChildType(String path) throws InterruptedException, KeeperException {
        String childWithType = path;
        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            childWithType += " [Folder]";
        } else {
            childWithType += " [File]";
        }
        return childWithType;
    }

    // Рекурсивный метод спуска вниз по иерархии
    private static void stepIn(String path, String step) throws InterruptedException, KeeperException {
        // Переменная step существует для разделения подпапок/файлов от их родительской папки
        step += "   ";

        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            String finalStep = step;
            zooKeeper.getChildren("/" + path,
                    false).forEach(childInner -> {
                try {
                    LOG.info(finalStep + getChildType(path + "/" + childInner, childInner));
//                  Вариант с полным выводом пути до ноды
//                    LOG.info(getChildType(path + "/" + childInner));
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
                try {
                    stepIn(path + "/" + childInner, finalStep);
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}