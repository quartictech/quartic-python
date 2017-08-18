
# Language runner spec
## Command
The language runner must support running in two modes: `execute` and `evaluate`.

### Modes
The runner may be run in one of two modes:
 - `--evaluate <file>`
    Extract steps from the code base and output them to `<file>` in JSON format.
 - `--execute <step_id>` 
    Execute step with id `<step_id>`. This must match the `id` of a step output during the `evaluate` stage.

### Options
 - `--namespace` 
    Specifies the namespace against which the code should be run (in execute mode). If supplied during evaluation it has no effect.
 - `--exception <exception_file>`
    Output structured exception information to `<exception_file>`.

