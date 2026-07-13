package com.example.NMS.availability;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AvailabilityTest {
    @Test
    void computesPercent() {
        assertEquals(0.0, Availability.recompute(0, 0), 0.001);
        assertEquals(50.0, Availability.recompute(1, 2), 0.001);
        assertEquals(100.0, Availability.recompute(4, 4), 0.001);
    }
}
