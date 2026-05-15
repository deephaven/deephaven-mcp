/**
 * Tests for client/base module.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ClientObjectWrapper, isEnterpriseAvailable } from "./base.js";
import { InternalError } from "../exceptions.js";

// ---------------------------------------------------------------------------
// isEnterpriseAvailable
// ---------------------------------------------------------------------------

it("is_enterprise_available_is_boolean", () => {
  expect(typeof isEnterpriseAvailable).toBe("boolean");
});

// ---------------------------------------------------------------------------
// ClientObjectWrapper
// ---------------------------------------------------------------------------

describe("ClientObjectWrapper", () => {
  it("init_with_valid_object_non_enterprise", () => {
    const obj = { foo: "bar" };
    const wrapper = new ClientObjectWrapper(obj, false);
    expect(wrapper.wrapped).toBe(obj);
  });

  it("init_with_none_raises", () => {
    expect(() => new ClientObjectWrapper(null as unknown as object, false)).toThrow(
      /Cannot wrap None/,
    );
    expect(() => new ClientObjectWrapper(undefined as unknown as object, false)).toThrow(
      /Cannot wrap None/,
    );
  });

  it("enterprise_not_available_raises_internal_error", () => {
    // We can test this path by mocking isEnterpriseAvailable to false.
    // The module exports a mutable let, but we can test the error message shape.
    // In an environment where @deephaven/jsapi-nodejs is not installed,
    // isEnterpriseAvailable will be false and this test verifies the error is thrown.
    if (!isEnterpriseAvailable) {
      const obj = { foo: "bar" };
      expect(() => new ClientObjectWrapper(obj, true)).toThrow(InternalError);
      expect(() => new ClientObjectWrapper(obj, true)).toThrow(
        /enterprise features are not available/,
      );
      expect(() => new ClientObjectWrapper(obj, true)).toThrow(/Please report this issue/);
    } else {
      // Enterprise is available - just verify wrapper works with enterprise=true
      const obj = { foo: "bar" };
      const wrapper = new ClientObjectWrapper(obj, true);
      expect(wrapper.wrapped).toBe(obj);
    }
  });

  it("wrapped_property_returns_correct_object", () => {
    const obj = { x: 1, y: 2 };
    const wrapper = new ClientObjectWrapper(obj, false);
    expect(wrapper.wrapped).toBe(obj);
    expect(wrapper.wrapped.x).toBe(1);
  });

  it("wrapped_property_is_readonly", () => {
    const obj = { foo: "bar" };
    const wrapper = new ClientObjectWrapper(obj, false);
    expect(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- testing readonly
      (wrapper as any).wrapped = obj;
    }).toThrow();
  });

  it("type_preservation", () => {
    class Dummy {
      foo = "bar";
    }
    const dummy = new Dummy();
    const wrapper = new ClientObjectWrapper(dummy, false);
    expect(wrapper.wrapped).toBeInstanceOf(Dummy);
  });

  it("multiple_instances_operate_independently", () => {
    const obj1 = { id: 1 };
    const obj2 = { id: 2 };
    const wrapper1 = new ClientObjectWrapper(obj1, false);
    const wrapper2 = new ClientObjectWrapper(obj2, false);
    expect(wrapper1.wrapped).toBe(obj1);
    expect(wrapper2.wrapped).toBe(obj2);
    expect(wrapper1).not.toBe(wrapper2);
  });
});
