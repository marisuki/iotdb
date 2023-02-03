package org.apache.iotdb.db.simd.fsm;

public class StateContext {
    private String currState;
    private StateContext next;

    public StateContext(String state, StateContext nextState) {
        currState = state;
        next = nextState;
    }

    public String getCurrState() {
        return currState;
    }

    public StateContext getNext() {
        return next;
    }

    public boolean hasNext() {
        return next != null;
    }
}
