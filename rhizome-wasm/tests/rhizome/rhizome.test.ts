import assert from 'assert'

export function runRhizomeTest(
  impl: {
    runner?: { describe, it },
    rhizome: {
      Rhizome,
      InputTuple
    }
  }) {

  // Use runner or fallback to implicit mocha implementations
  const describe = impl.runner?.describe ?? globalThis.describe
  const it = impl.runner?.it ?? globalThis.it

  const { Rhizome, InputTuple } = impl.rhizome

  describe('rhizome', async () => {
    it('can compute simple projections', async () => {
      const client = await new Rhizome((p) => {
        p.output("values", { value: "int" });

        p.rule(
          "values",
          (value) => ({ value }),
          (value) => [
            {
              op: "search",
              rel: "evac",
              where: {
                attribute: "value",
                value,
              },
            },
          ]
        );
      });

      const values = [];

      await client.registerSink("values", (f) => {
        values.push(f.value);
      });

      let resolver;
      const p = new Promise((resolve) => { resolver = resolve });

      await client.registerStream("evac", async function*() {
        yield new InputTuple("1", "value", 1, {});
        yield new InputTuple("1", "value", 2, {});
        yield new InputTuple("1", "value", 3, {});
        yield new InputTuple("1", "value", 4, {});
        yield new InputTuple("1", "value", 5, {});

        resolver();
      }());

      await p;
      await client.flush();

      assert.deepStrictEqual(values, [1, 2, 3, 4, 5]);
    })
  })
}
