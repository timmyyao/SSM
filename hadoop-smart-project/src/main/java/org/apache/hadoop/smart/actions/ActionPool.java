package org.apache.hadoop.smart.actions;

/**
 * Action Pool
 */
public class ActionPool {

    private static ActionPool instance = new ActionPool();

    private ActionPool() {
    }

    public static ActionPool getInstance() {
        return  instance;
    }

    public void getActionStatus() {

    }

}
