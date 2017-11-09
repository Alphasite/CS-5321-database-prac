package db.datastore.index;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class BTreeTest {

    Path indexFile = Paths.get("resources/samples-4/expected_indexes/Boats.E");

    @Test
    public void testLoadIndex() {
        BTree indexTree = BTree.createTree(indexFile);

        assertEquals(10, indexTree.getOrder());
        assertEquals(316, indexTree.getNbLeaves());

        IndexNode rootNode = indexTree.getRoot();

        assertEquals(15, rootNode.keys.length);
        assertEquals(16, rootNode.children.length);

        assertEquals(665, rootNode.keys[0]);
        assertEquals(1330, rootNode.keys[1]);
        assertEquals(9665, rootNode.keys[14]);

        assertEquals(317, rootNode.children[0]);
        assertEquals(318, rootNode.children[1]);
        assertEquals(332, rootNode.children[15]);
    }

    @Test
    public void testSearch() {
        BTree indexTree = BTree.createTree(indexFile);

        DataEntry res = indexTree.search(4);
        assertEquals(3, res.rids.length);
        assertEquals(6, res.rids[0].pageid);
        assertEquals(272, res.rids[0].tupleid);

        res = indexTree.search(2504);
        assertEquals(6, res.rids.length);
        assertEquals(26, res.rids[5].pageid);
        assertEquals(329, res.rids[5].tupleid);

        res = indexTree.search(1453);
        assertEquals(null, res);
    }
}
